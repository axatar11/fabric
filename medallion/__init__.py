"""Shared medallion pipeline library for Fabric, Databricks, and local Spark."""

__all__ = ["init_notebook"]


def __getattr__(name: str):
    if name == "init_notebook":
        from medallion.notebook_init import init_notebook

        return init_notebook
    raise AttributeError(name)
