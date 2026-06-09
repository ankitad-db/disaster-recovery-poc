"""CLI entrypoint: ``python -m databricks_dr <module> <action>`` / ``databricks-dr ...``.

Generic over modules: any registered object type exposes the same verbs
(seed/baseline/cdc/failover/failback/validate), so adding a module needs no CLI change.
"""

from __future__ import annotations

import click

from .common.audit import AuditLog
from .common.config import load_config
from .common.logging import configure, get_logger
from .core.base import RunContext
from .core import registry

_logger = get_logger(__name__)
_ACTIONS = ("seed", "baseline", "cdc", "failover", "failback", "validate")


def _build_context(config_path: str | None, failback: bool, triggered_by: str, dry_run: bool) -> RunContext:
    cfg = load_config(config_path)
    direction = cfg.direction(failback=failback)
    audit = AuditLog(table=cfg.audit_table)
    return RunContext(cfg=cfg, direction=direction, audit=audit, triggered_by=triggered_by, dry_run=dry_run)


@click.group()
@click.option("--config", "config_path", default=None, help="Path to dr_config.yaml")
@click.option("--log-level", default=None, help="DEBUG|INFO|WARNING|ERROR")
@click.pass_context
def main(ctx: click.Context, config_path: str | None, log_level: str | None):
    """Databricks DR framework."""
    configure(log_level)
    ctx.ensure_object(dict)
    ctx.obj["config_path"] = config_path


@main.group()
def config():
    """Inspect configuration."""


@config.command("show")
@click.pass_context
def config_show(ctx: click.Context):
    cfg = load_config(ctx.obj["config_path"])
    fwd = cfg.direction()
    back = cfg.direction(failback=True)
    click.echo(f"config: {cfg.path}")
    click.echo(f"active primary: {cfg.active_primary_key()}  secondary: {cfg.secondary_key()}")
    click.echo(f"sync direction   : {fwd.label}  (folder={fwd.folder})")
    click.echo(f"failback direction: {back.label}  (folder={back.folder})")
    click.echo(f"audit table: {cfg.audit_table}")
    click.echo(f"modules available: {registry.available()}")


@main.command()
def modules():
    """List registered DR modules."""
    for m in registry.available():
        click.echo(m)


def _make_action(action: str):
    @click.option("--config", "config_path", default=None, help="Path to dr_config.yaml")
    @click.option("--failback", is_flag=True, default=False, help="Run in the failback direction (secondary->primary)")
    @click.option("--triggered-by", default="MANUAL", help="SCHEDULE|AUDIT_EVENT|MANUAL")
    @click.option("--dry-run", is_flag=True, default=False)
    @click.pass_context
    def _cmd(ctx, config_path, failback, triggered_by, dry_run):
        object_type = ctx.obj["object_type"]
        cfg_path = config_path or ctx.obj.get("config_path")
        run_ctx = _build_context(cfg_path, failback, triggered_by, dry_run)
        module_cls = registry.get_module(object_type)
        module = module_cls(run_ctx)
        _logger.info("Running %s.%s direction=%s dry_run=%s", object_type, action, run_ctx.direction.label, dry_run)
        getattr(module, action)()

    _cmd.__name__ = action
    return _cmd


@main.group()
@click.pass_context
def models(ctx: click.Context):
    """Models DR module."""
    ctx.obj["object_type"] = "models"


for _action in _ACTIONS:
    models.command(_action)(_make_action(_action))


if __name__ == "__main__":
    main()
