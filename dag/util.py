import inspect
from typing import Any, Callable
import os

from dagster_shell import execute_shell_command
from dagster import Field, op, asset, Failure


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
