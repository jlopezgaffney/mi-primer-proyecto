# Databricks notebook source
"""Configuración común del TP final SEPA (se carga con %run ./00_config)."""

from dataclasses import dataclass
import re


def _identifier(value: str, what: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(f"{what} inválido: {value!r}. Usá letras, números y _.")
    return value


@dataclass(frozen=True)
class SepaConfig:
    catalog: str
    schema: str = "sepa_tp"
    volume: str = "landing"

    def __post_init__(self):
        _identifier(self.catalog, "catálogo")
        _identifier(self.schema, "esquema")
        _identifier(self.volume, "volumen")

    @property
    def namespace(self) -> str:
        return f"`{self.catalog}`.`{self.schema}`"

    @property
    def volume_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"

    @property
    def incoming(self) -> str:
        """Carpeta donde caen los zips diarios."""
        return f"{self.volume_path}/incoming"

    def checkpoint(self, name: str) -> str:
        return f"{self.volume_path}/_checkpoints/{name}"

    def table(self, name: str) -> str:
        return f"{self.namespace}.`{_identifier(name, 'tabla')}`"


def create_namespace(spark, cfg: SepaConfig) -> None:
    """Crea (si no existen) el esquema y el volumen. No borra nada."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.namespace}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {cfg.namespace}.`{cfg.volume}`")


def table_count(spark, table: str) -> int:
    return spark.table(table).count() if spark.catalog.tableExists(table) else 0
