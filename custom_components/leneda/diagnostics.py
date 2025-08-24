"""Diagnostics support for Leneda."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_API_TOKEN
from homeassistant.core import HomeAssistant
from homeassistant.helpers.redact import async_redact_data

from .const import CONF_ENERGY_ID, CONF_METERING_POINT

TO_REDACT = [CONF_ENERGY_ID, CONF_API_TOKEN, CONF_METERING_POINT, "title", "unique_id"]


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, config_entry: ConfigEntry
) -> Mapping[str, Any]:
    """Return diagnostics for a config entry."""
    coordinator = config_entry.runtime_data
    coordinator_data = coordinator.data

    # Anonymize metering point keys
    def anonymize_key(key: str) -> str:
        if len(key) <= 6:
            return key
        return key[:6] + ("#" * (len(key) - 6))

    # Anonymize metering points in data
    anonymized_coordinator_data = (
        {anonymize_key(mp): value for mp, value in coordinator_data.items()}
        if isinstance(coordinator_data, dict)
        else coordinator_data
    )

    return {
        "config_entry_data": async_redact_data(dict(config_entry.data), TO_REDACT),
        "config_entry_options": async_redact_data(
            dict(config_entry.options), TO_REDACT
        ),
        "config_entry_unique_id": config_entry.unique_id,
        "config_subentries": async_redact_data(
            dict(config_entry.subentries), TO_REDACT
        ),
        "coordinator_data": anonymized_coordinator_data,
    }
