import inspect
from typing import Any, Callable
import os

from dagster_shell import execute_shell_command
from dagster import Field, op, asset, Failure

def parse_signature(f: Callable) -> list[tuple[str, type, Any, bool]]:
    """
    Returns [name, type, default, has_default]
    """
    sig = inspect.signature(f)
    # Handle inputs types
    assert not any(p.kind == inspect.Parameter.POSITIONAL_ONLY for p in sig.parameters.values()), \
        'Cannot use positional-only parameters'
    inputs = [
        (name, param.annotation, param.default, param.default==inspect._empty) 
        for name, param in sig.parameters.items()
    ]
    return inputs

def f_to_asset(f, **asset_kwargs):
    """
    Insanely, dagster's function arguments are for the *data* (assets) and we have to infect
    our op definitions with the poorly-considered configuration API to statically 
    reuse an op with separate arguments. Or we could turn around and make table names and urls
    into assets themselves but that would be deeply stupid.

    Obviously the way this should work is that assets by default are pointers to data (which could
    just be pathnames), and assets have no return value by default. An actual value is a special case,\
    not the norm. This means that in a graph, the variable names and not the function definitions
    correspond to actual assets, which is of course how code works.
    """
    sig = parse_signature(f)
    config_schema = {
        name: Field(t, is_required=False, default_value=default)
            if has_default
            else Field(t, is_required=True)
        for (name, t, default, has_default) in sig
        if name != 'context'
    }

    @asset(config_schema=config_schema, **asset_kwargs)
    def wrapper(context):
        f_kwargs = {
            name: context.op_config[name]
            for name, f in config_schema.items()
            if f.is_required or name in context.op_config
        }
        return f(**f_kwargs)
    return wrapper


def create_shell_command_asset(cmd, **asset_kwargs):
    """
    Copied from create_shell_command_op bc there's no good way to convert an op to an asset
    (loses some params)
    Why can't I pass arguments to commands?
    Why doesn't the basic 'execute_shell_command' pass envars or error on nonzero?
    """

    @asset(**asset_kwargs)
    def _asset(context) -> None:
        op_config = context.op_config.copy() if context.op_config else {}
        op_config["env"] = {**os.environ, **op_config.get("env", {})}
        output, return_code = execute_shell_command(
            shell_command=cmd,
            output_logging='BUFFER', 
            log=context.log,
            **op_config
        )

        if return_code:
            raise Failure(
                description="Shell command execution failed with output: {output}".format(
                    output=output
                )
            )
    return _asset
