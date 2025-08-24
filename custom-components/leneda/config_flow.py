"""Config flow for Leneda integration."""

from __future__ import annotations

from asyncio import Task
from collections.abc import Mapping
from datetime import datetime, timedelta
import logging
from typing import Any, Final

from leneda import LenedaClient
from leneda.exceptions import (
    ForbiddenException,
    MeteringPointNotFoundException,
    UnauthorizedException,
)
from leneda.models import AuthenticationProbeResult
from leneda.obis_codes import ObisCode
import voluptuous as vol

from homeassistant import config_entries
from homeassistant.config_entries import (
    ConfigFlowResult,
    ConfigSubentryFlow,
    SubentryFlowResult,
)
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import CONF_API_TOKEN, CONF_ENERGY_ID, DOMAIN, SENSOR_TYPES

_LOGGER = logging.getLogger(__name__)

# Setup types
SETUP_TYPE_PROBE: Final = "probe"
SETUP_TYPE_MANUAL: Final = "manual"

# Error messages
ERROR_INVALID_METERING_POINT: Final = "invalid_metering_point"
ERROR_SELECT_AT_LEAST_ONE: Final = "select_at_least_one"
ERROR_DUPLICATE_METERING_POINT: Final = "duplicate_metering_point"
ERROR_FORBIDDEN: Final = "forbidden"
ERROR_UNAUTHORIZED: Final = "unauthorized"


class LenedaConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Leneda (main entry: authentication only)."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the config flow."""
        self._api_token: str = ""
        self._energy_id: str = ""

    @staticmethod
    @callback
    def async_get_supported_subentry_types(config_entry: config_entries.ConfigEntry):
        """Get the supported subentry types for this handler."""
        # Register the subentry type and handler
        return {
            "metering_point": LenedaSubEntryFlowHandler,
        }

    async def async_step_reauth(
        self, entry_data: Mapping[str, Any]
    ) -> ConfigFlowResult:
        """Handle reauthentication flow when API token becomes invalid."""
        self._energy_id = entry_data[CONF_ENERGY_ID]
        return await self.async_step_reauth_confirm()

    async def async_step_reauth_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reauthentication confirmation step."""
        errors: dict[str, str] = {}
        if user_input is not None:
            self._api_token = user_input[CONF_API_TOKEN]

            # Validate new API token
            try:
                client = LenedaClient(
                    api_key=self._api_token,
                    energy_id=self._energy_id,
                )

                credentials_probe_result = await client.probe_credentials()

                if credentials_probe_result != AuthenticationProbeResult.FAILURE:
                    if credentials_probe_result == AuthenticationProbeResult.UNKNOWN:
                        _LOGGER.warning(
                            "Unknown authentication probe result for energy ID %s. As credentials might be valid, we'll try to use them anyway",
                            self._energy_id,
                        )
                    return self.async_update_reload_and_abort(
                        self._get_reauth_entry(),
                        data_updates={CONF_API_TOKEN: self._api_token},
                    )

                errors = {"base": ERROR_UNAUTHORIZED}
            except ForbiddenException:
                errors = {"base": ERROR_FORBIDDEN}

        return self.async_show_form(
            step_id="reauth_confirm",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_API_TOKEN): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.PASSWORD,
                            autocomplete="leneda-api-token",
                        )
                    ),
                }
            ),
            description_placeholders={"energy_id": self._energy_id},
            errors=errors,
        )

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step of the config flow."""
        errors: dict[str, str] = {}
        if user_input is not None:
            await self.async_set_unique_id(user_input[CONF_ENERGY_ID])
            self._abort_if_unique_id_configured()
            self._api_token = user_input[CONF_API_TOKEN]
            self._energy_id = user_input[CONF_ENERGY_ID]

            # Validate authentication by making a test API call
            try:
                client = LenedaClient(
                    api_key=self._api_token,
                    energy_id=self._energy_id,
                )

                credentials_probe_result = await client.probe_credentials()

                if credentials_probe_result != AuthenticationProbeResult.FAILURE:
                    if credentials_probe_result == AuthenticationProbeResult.UNKNOWN:
                        _LOGGER.warning(
                            "Unknown authentication probe result for energy ID %s. As credentials might be valid, we'll try to use them anyway",
                            self._energy_id,
                        )
                    return self.async_create_entry(
                        title=self._energy_id,
                        data={
                            CONF_API_TOKEN: self._api_token,
                            CONF_ENERGY_ID: self._energy_id,
                        },
                    )

                errors = {"base": ERROR_UNAUTHORIZED}
            except ForbiddenException:
                errors = {"base": ERROR_FORBIDDEN}

        return self.async_show_form(
            step_id="user",
            data_schema=vol.Schema(
                {
                    vol.Required(CONF_API_TOKEN): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.PASSWORD,
                            autocomplete="leneda-api-token",
                        )
                    ),
                    vol.Required(CONF_ENERGY_ID): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.TEXT,
                            autocomplete="leneda-energy-id",
                        )
                    ),
                }
            ),
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_config_entry_title(config_entry: config_entries.ConfigEntry) -> str:
        """Get the title for the config entry."""
        return config_entry.data.get(CONF_ENERGY_ID, "Leneda")


class LenedaSubEntryFlowHandler(ConfigSubentryFlow):
    """Handle subentry flow for metering points and sensors (subentry type: 'metering_point')."""

    VERSION = 1

    def __init__(self) -> None:
        """Initialize the subentry flow handler."""
        self._metering_point: str = ""
        self._selected_sensors: list[str] = []
        self._probing_task: Task | None = None

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Alias user step to init for subentry flow compatibility."""
        return await self.async_step_init(user_input)

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the initial step of the subentry flow."""
        errors = {}
        if user_input is not None:
            self._metering_point = user_input["metering_point"].strip()
            if not self._metering_point:
                errors["base"] = ERROR_INVALID_METERING_POINT
            else:
                # Check for duplicate metering point across all config entries for the integration domain
                for parent_entry in self.hass.config_entries.async_entries(DOMAIN):
                    # Check subentries (if this is a parent entry with subentries)
                    for subentry in parent_entry.subentries.values():
                        if subentry.data.get("metering_point") == self._metering_point:
                            return self.async_abort(
                                reason=ERROR_DUPLICATE_METERING_POINT
                            )
                if not errors:
                    try:
                        parent_entry = self._get_entry()
                        api_token = parent_entry.data[CONF_API_TOKEN]
                        energy_id = parent_entry.data[CONF_ENERGY_ID]

                        client = LenedaClient(
                            api_key=api_token,
                            energy_id=energy_id,
                        )

                        start_date = datetime.now() - timedelta(days=7)
                        end_date = datetime.now()

                        await client.get_aggregated_metering_data(
                            self._metering_point,
                            ObisCode.ELEC_CONSUMPTION_ACTIVE,
                            start_date,
                            end_date,
                            "Hour",
                            "Accumulation",
                        )
                    except MeteringPointNotFoundException:
                        errors["base"] = ERROR_INVALID_METERING_POINT
                    else:
                        return await self.async_step_setup_type()
        return self.async_show_form(
            step_id="init",
            data_schema=vol.Schema(
                {
                    vol.Required("metering_point"): selector.TextSelector(
                        selector.TextSelectorConfig(
                            type=selector.TextSelectorType.TEXT,
                            autocomplete="leneda-metering-point",
                        )
                    ),
                }
            ),
            errors=errors,
        )

    async def async_step_setup_type(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the setup type selection step."""
        return self.async_show_menu(
            step_id="setup_type",
            menu_options=[SETUP_TYPE_PROBE, SETUP_TYPE_MANUAL],
            description_placeholders={"metering_point": self._metering_point},
        )

    async def async_step_probe(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the probe step to detect available sensors."""
        _LOGGER.debug("Starting probe for metering point %s", self._metering_point)
        if not self._probing_task:
            # Get parent entry for auth just for this API call
            parent_entry = self._get_entry()
            api_token = parent_entry.data[CONF_API_TOKEN]
            energy_id = parent_entry.data[CONF_ENERGY_ID]
            self._probing_task = self.hass.async_create_task(
                self._fetch_obis_codes(api_token, energy_id)
            )
        if not self._probing_task.done():
            return self.async_show_progress(
                progress_action="fetch_obis",
                progress_task=self._probing_task,
            )
        try:
            supported_obis_codes = await self._probing_task
            self._probing_task = None
            if not supported_obis_codes:
                return self.async_show_progress_done(next_step_id="probe_no_sensors")
            detected_sensors = []
            for obis_code in supported_obis_codes:
                for sensor_type, cfg in SENSOR_TYPES.items():
                    if cfg["obis_code"] == obis_code:
                        detected_sensors.append(sensor_type)
                        break
            self._selected_sensors = detected_sensors
            return self.async_show_progress_done(next_step_id=SETUP_TYPE_MANUAL)
        except UnauthorizedException:
            self._probing_task = None
            return self.async_abort(reason=ERROR_UNAUTHORIZED)
        except ForbiddenException:
            self._probing_task = None
            return self.async_abort(reason=ERROR_FORBIDDEN)

    async def _fetch_obis_codes(self, api_token: str, energy_id: str) -> list[ObisCode]:
        """Fetch supported OBIS codes from the Leneda API."""
        client = LenedaClient(
            api_key=api_token,
            energy_id=energy_id,
        )
        return await client.get_supported_obis_codes(self._metering_point)

    async def async_step_probe_no_sensors(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the case when no sensors are detected during probing."""
        return self.async_show_menu(
            step_id="probe_no_sensors",
            menu_options=[SETUP_TYPE_MANUAL],
            description_placeholders={"metering_point": self._metering_point},
        )

    async def async_step_manual(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the manual sensor selection step."""
        if user_input is not None:
            selected_sensors = user_input.get("sensors", [])
            if not selected_sensors:
                return self.async_show_form(
                    step_id=SETUP_TYPE_MANUAL,
                    data_schema=_get_manual_sensor_selection_schema([]),
                    description_placeholders={
                        "metering_point": self._metering_point,
                        "probed_text": "",
                    },
                    errors={"base": ERROR_SELECT_AT_LEAST_ONE},
                )
            self._selected_sensors = selected_sensors
            return await self.async_step_finish()
        default_sensors = self._selected_sensors or []
        return self.async_show_form(
            step_id=SETUP_TYPE_MANUAL,
            data_schema=_get_manual_sensor_selection_schema(default_sensors),
            description_placeholders={
                "metering_point": self._metering_point,
                "probed_text": " (pre-selected based on probing)"
                if default_sensors
                else "",
            },
        )

    async def async_step_finish(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """Handle the final step of the subentry flow."""
        # Only store metering_point and sensors in the subentry's data
        return self.async_create_entry(
            title=self._metering_point,
            unique_id=self._metering_point,
            data={
                "metering_point": self._metering_point,
                "sensors": self._selected_sensors,
            },
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """User flow to modify an existing metering point."""
        return await self.async_step_configure_sensors()

    async def async_step_configure_sensors(
        self, user_input: dict[str, Any] | None = None
    ) -> SubentryFlowResult:
        """User flow to modify an existing metering point."""
        # Retrieve the parent config entry for reference.
        config_entry = self._get_entry()
        # Retrieve the specific subentry targeted for update.
        config_subentry = self._get_reconfigure_subentry()

        metering_point = config_subentry.data.get("metering_point", "")
        current_sensors = config_subentry.data.get("sensors", [])

        if user_input is not None:
            if not user_input["sensors"]:
                return self.async_show_form(
                    step_id="configure_sensors",
                    data_schema=_get_manual_sensor_selection_schema(current_sensors),
                    description_placeholders={
                        "metering_point": metering_point,
                    },
                    errors={"base": "select_at_least_one"},
                )

            # Update the config entry
            return self.async_update_and_abort(
                config_entry,
                config_subentry,
                data={
                    **config_subentry.data,
                    "sensors": user_input["sensors"],
                },
            )

        return self.async_show_form(
            step_id="configure_sensors",
            data_schema=_get_manual_sensor_selection_schema(current_sensors),
            description_placeholders={
                "metering_point": metering_point,
            },
        )


def _get_manual_sensor_selection_schema(
    selected_sensors: list[str] | None = None,
) -> vol.Schema:
    """Get the schema for manual sensor selection."""
    return vol.Schema(
        {
            vol.Required(
                "sensors",
                default=selected_sensors or [],
            ): selector.SelectSelector(
                selector.SelectSelectorConfig(
                    options=list(SENSOR_TYPES.keys()),
                    multiple=True,
                    mode=selector.SelectSelectorMode.LIST,
                    translation_key="sensors",
                )
            ),
        }
    )
