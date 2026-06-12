#!/usr/bin/env python3
# Initialize Protection Groups - One-time setup for Pure Storage FlashArray PG-based snapshots
#
# Connects to the FA gateway, discovers current cluster nodes from Ops Manager (with .env fallback),
# resolves the FlashArray volume backing /data/mongo on each node via SCSI serial, creates a
# Protection Group named $ProtectionGroupName on each array, and adds each volume as a member.
# Safe to re-run - skips creation if the PG or membership already exists.
#
# Use -Prune to remove PG volume members that no longer correspond to any discovered cluster node.
#
# Usage:
#   init-protection-groups
#   init-protection-groups --what-if           # dry run
#   init-protection-groups --prune             # remove orphaned PG members (typed confirmation required)
#   init-protection-groups --prune --what-if   # dry run with prune preview
#   init-protection-groups --prune --force     # skip the typed confirmation (use only in automation)

import typer

from . import config


def _run(
    what_if: bool = typer.Option(
        False, "--what-if", help="Show what would be created without making changes"
    ),
    prune: bool = typer.Option(
        False,
        "--prune",
        help="Remove PG volume members whose volumes no longer correspond to any current cluster node",
    ),
    force: bool = typer.Option(
        False,
        "--force",
        help="Skip the typed-token confirmation prompt for --prune (destructive)",
    ),
    deployment: str = typer.Option(
        None,
        "--deployment",
        help="Deployment name to initialize (selects '<NAME>__' keys in .env). Omit to use the flat keys.",
    ),
) -> None:
    # Load env-derived config FIRST.
    config.load_config(deployment=deployment)

    # WhatIf banner.
    if what_if:
        config.write_host("\n[WhatIf] No changes will be made.", fg=config.DARK_YELLOW)

    # Header lines.
    config.write_host("\n=== Initialize Protection Groups ===", fg=config.YELLOW)
    config.write_host(
        f"  Protection group  : {config.CFG.ProtectionGroupName}", fg=config.CYAN
    )
    config.write_host(f"  Gateway           : {config.CFG.FaEndpoint}", fg=config.CYAN)

    # Connect to the gateway.
    fa = config.connect_fa()
    config.write_host("  Connected to gateway.", fg=config.GREEN)

    # Enumerate fleet arrays.
    config.write_host("  Enumerating fleet arrays...", fg=config.CYAN)
    fleet_members = config._fa(fa.get_fleets_members())
    all_arrays = []
    for fm in fleet_members:
        member = getattr(fm, "member", None)
        name = getattr(member, "name", None) if member is not None else None
        if name:
            if name not in all_arrays:  # Unique (preserves first-seen order)
                all_arrays.append(name)
    config.write_host(
        f"  Fleet arrays ({len(all_arrays)}): {', '.join(all_arrays)}", fg=config.CYAN
    )

    # Discover cluster nodes (Ops Manager first, .env fallback).
    config.write_host("  Discovering cluster nodes...", fg=config.CYAN)
    cluster_nodes = config.get_cluster_nodes()

    # Keep the .env CLUSTER_NODES fallback fresh: persist the discovered node list back to the
    # deployment-scoped key so a later run still works if Ops Manager is unreachable. No-op when the
    # list is unchanged (e.g. discovery already fell back to .env).
    if not what_if and cluster_nodes:
        nodes_key = config.deployment_env_key(config.CFG.DeploymentName, "CLUSTER_NODES")
        if config.update_env_var(config.CFG.EnvFilePath, nodes_key, ",".join(cluster_nodes)):
            config.write_host(
                f"  Updated {nodes_key} in {config.CFG.EnvFilePath.name} "
                f"({len(cluster_nodes)} nodes).",
                fg=config.GREEN,
            )

    # Discover node-to-volume mappings via SCSI serial.
    all_context_names = list(all_arrays)
    config.write_host(
        "  Discovering node-to-volume mappings via SCSI serial...", fg=config.CYAN
    )
    node_volume_map = config.resolve_node_to_array_volume_map(
        fa,
        cluster_nodes,
        config.CFG.SshUser,
        config.SSH_OPTS,
        all_context_names,
    )

    # Persist the node->volume map as copyable FA volume tags so snapshot/restore can read it without
    # per-node SSH/SCSI discovery (the slow path at scale). Re-run after any topology change.
    if what_if:
        config.write_host(
            f"  [WhatIf] Would tag {len(node_volume_map)} volume(s) with the node->volume map "
            f"(deployment={config.CFG.DeploymentName or '(default)'}).",
            fg=config.DARK_YELLOW,
        )
    else:
        config.write_host("  Writing node->volume map tags...", fg=config.CYAN)
        config.write_volume_map_tags(fa, config.CFG.DeploymentName, node_volume_map)

    errors: list[str] = []

    # Iterate node->volume map, create PG + member, verify.
    for node, entry in node_volume_map.items():
        short_name = entry["ShortName"]
        volume_name = entry["VolumeName"]

        config.write_host(
            f"\n  [{node} -> {short_name} / {volume_name}]", fg="white"
        )
        try:
            # Create the PG if it does not exist on this array.
            existing_pg = config._fa(
                fa.get_protection_groups(
                    names=[config.CFG.ProtectionGroupName],
                    context_names=[short_name],
                ),
                allow_error=True,
            )
            if existing_pg:
                config.write_host(
                    f"    PG '{config.CFG.ProtectionGroupName}' already exists - skipping creation",
                    fg=config.DARK_GRAY,
                )
            else:
                if what_if:
                    config.write_host(
                        f"    [WhatIf] Would create PG '{config.CFG.ProtectionGroupName}'",
                        fg=config.DARK_YELLOW,
                    )
                else:
                    config._fa(
                        fa.post_protection_groups(
                            names=[config.CFG.ProtectionGroupName],
                            context_names=[short_name],
                        )
                    )
                    config.write_host(
                        f"    Created PG '{config.CFG.ProtectionGroupName}'",
                        fg=config.GREEN,
                    )

            # Add the volume as a member if not already present.
            existing_member = config._fa(
                fa.get_protection_groups_volumes(
                    group_names=[config.CFG.ProtectionGroupName],
                    member_names=[volume_name],
                    context_names=[short_name],
                ),
                allow_error=True,
            )
            if existing_member:
                config.write_host(
                    f"    Volume '{volume_name}' is already a member - skipping",
                    fg=config.DARK_GRAY,
                )
            else:
                if what_if:
                    config.write_host(
                        f"    [WhatIf] Would add volume '{volume_name}' to PG '{config.CFG.ProtectionGroupName}'",
                        fg=config.DARK_YELLOW,
                    )
                else:
                    config._fa(
                        fa.post_protection_groups_volumes(
                            group_names=[config.CFG.ProtectionGroupName],
                            member_names=[volume_name],
                            context_names=[short_name],
                        )
                    )
                    config.write_host(
                        f"    Added '{volume_name}' to PG '{config.CFG.ProtectionGroupName}'",
                        fg=config.GREEN,
                    )

            # Verify final state (skipped under WhatIf).
            if not what_if:
                members = config._fa(
                    fa.get_protection_groups_volumes(
                        group_names=[config.CFG.ProtectionGroupName],
                        context_names=[short_name],
                    ),
                    allow_error=True,
                )
                member_names = [m.member.name for m in members]
                config.write_host(
                    f"    PG members: {', '.join(member_names)}", fg=config.CYAN
                )

        except Exception as e:
            errors.append(f"{node} ({short_name}): {e}")
            config.write_host(f"    ERROR: {e}", fg=config.RED)

    # Prune orphaned PG volume members.
    if prune:
        config.write_host("\n=== Pruning orphaned PG members ===", fg=config.YELLOW)
        discovered_volume_names = [v["VolumeName"] for v in node_volume_map.values()]

        # Enumerate all planned removals across the fleet.
        prune_targets: list[dict[str, str]] = []
        for arr in all_arrays:
            pg = config._fa(
                fa.get_protection_groups(
                    names=[config.CFG.ProtectionGroupName],
                    context_names=[arr],
                ),
                allow_error=True,
            )
            if not pg:
                continue
            members = config._fa(
                fa.get_protection_groups_volumes(
                    group_names=[config.CFG.ProtectionGroupName],
                    context_names=[arr],
                ),
                allow_error=True,
            )
            for member in members:
                member_volume_name = member.member.name
                if member_volume_name not in discovered_volume_names:
                    prune_targets.append({"Array": arr, "Volume": member_volume_name})

        # No targets vs. planned removals listing.
        if len(prune_targets) == 0:
            config.write_host(
                "  No orphaned members found - nothing to prune.", fg=config.GREEN
            )
        else:
            config.write_host(
                f"  Planned removals ({len(prune_targets)}):", fg=config.CYAN
            )
            for t in prune_targets:
                config.write_host(f"    {t['Array']}: {t['Volume']}", fg="white")

            # Typed-token confirmation unless WhatIf or Force.
            proceed = what_if or force
            if not proceed:
                config.write_host(
                    f"\n  This will remove {len(prune_targets)} member(s) from PG '{config.CFG.ProtectionGroupName}'.",
                    fg=config.YELLOW,
                )
                config.write_host(
                    "  Future PG snapshots will NOT include those volumes.",
                    fg=config.YELLOW,
                )
                token = input(
                    f"  Type the PG name '{config.CFG.ProtectionGroupName}' to confirm prune: "
                ).strip()
                if token != config.CFG.ProtectionGroupName:
                    raise RuntimeError(
                        f"Confirmation token did not match '{config.CFG.ProtectionGroupName}'. Aborting prune."
                    )
                proceed = True

            # Perform removals.
            for t in prune_targets:
                if what_if:
                    config.write_host(
                        f"  [WhatIf] Would remove orphaned member '{t['Volume']}' from PG '{config.CFG.ProtectionGroupName}' on {t['Array']}",
                        fg=config.DARK_YELLOW,
                    )
                else:
                    try:
                        config._fa(
                            fa.delete_protection_groups_volumes(
                                group_names=[config.CFG.ProtectionGroupName],
                                member_names=[t["Volume"]],
                                context_names=[t["Array"]],
                            )
                        )
                        config.write_host(
                            f"  Removed orphaned member '{t['Volume']}' from {t['Array']}",
                            fg=config.GREEN,
                        )
                    except Exception as e:
                        errors.append(
                            f"prune {t['Volume']} on {t['Array']}: {e}"
                        )
                        config.write_host(
                            f"  ERROR pruning '{t['Volume']}': {e}", fg=config.RED
                        )

    # Aggregate errors.
    if len(errors) > 0:
        joined = "\n".join(errors)
        raise RuntimeError(
            f"Initialization failed on {len(errors)} item(s):\n{joined}"
        )

    # Completion banner.
    config.write_host("\n=== Initialization Complete ===", fg=config.GREEN)
    if what_if:
        config.write_host(
            "  WhatIf mode - no changes were made.", fg=config.DARK_YELLOW
        )
    else:
        config.write_host(
            f"  Protection group '{config.CFG.ProtectionGroupName}' is ready on all arrays.",
            fg=config.GREEN,
        )
        config.write_host("  You can now run new-mongo-snapshot.", fg="white")


def main() -> None:
    typer.run(_run)


if __name__ == "__main__":
    main()
