"""The Leneda coordinator for handling meter data and statistics."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import re
from typing import Any, cast

from leneda import LenedaClient
from leneda.exceptions import UnauthorizedException
from leneda.obis_codes import ObisCode, get_obis_info

from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
    statistics_during_period,
)
from homeassistant.components.recorder.util import get_instance
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .const import (
    DOMAIN,
    SCAN_INTERVAL,
    SENSOR_TYPES,
    UNIT_TO_AGGREGATED_UNIT,
    STATISTICS_PERIOD_START,
)

_LOGGER = logging.getLogger(__name__)


def _create_statistic_id(metering_point: str, obis: str) -> str:
    """Create a valid statistic ID from metering point and OBIS code.

    Args:
        metering_point: The metering point identifier
        obis: The OBIS code

    Returns:
        A formatted statistic ID string

    """
    clean_mp = re.sub(r"[^a-z0-9]", "_", metering_point.lower())
    clean_obis = re.sub(r"[^a-z0-9]", "_", obis.lower())
    statistic_id = f"{DOMAIN}:{clean_mp}_{clean_obis}"
    _LOGGER.debug(
        "Created statistic ID: %s from metering_point: %s, obis: %s",
        statistic_id,
        metering_point,
        obis,
    )
    return statistic_id


class LenedaCoordinator(DataUpdateCoordinator[dict[str, dict[str, Any]]]):
    """Handle fetching Leneda data, updating sensors and inserting statistics."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        api_token: str,
        energy_id: str,
    ) -> None:
        """Initialize the coordinator for all metering point subentries.

        Args:
            hass: Home Assistant instance
            config_entry: Configuration entry
            api_token: API token for authentication
            energy_id: Energy ID for the client

        """
        super().__init__(
            hass,
            _LOGGER,
            config_entry=config_entry,
            name=DOMAIN,
            update_interval=SCAN_INTERVAL,
        )
        self.client = LenedaClient(
            api_key=api_token,
            energy_id=energy_id,
        )
        self._initialize_metering_points(config_entry)

    def _initialize_metering_points(self, config_entry: ConfigEntry) -> None:
        """Initialize metering points from config entry subentries.

        Args:
            config_entry: Configuration entry containing metering points

        """
        self.metering_points = {}
        for subentry in config_entry.subentries.values():
            metering_point = subentry.data["metering_point"]
            sensors = subentry.data["sensors"]
            self.metering_points[metering_point] = sensors
            _LOGGER.debug(
                "Added metering point %s with sensors: %s",
                metering_point,
                sensors,
            )

    async def _async_update_data(self) -> dict[str, dict[str, Any]]:
        """Fetch data from Leneda API and update statistics for all metering points.

        Returns:
            Dictionary containing updated meter data

        Raises:
            ConfigEntryAuthFailed: If authentication fails

        """
        _LOGGER.debug("Starting data update for all metering points")
        data = {}

        try:
            for metering_point, selected_sensors in self.metering_points.items():
                data[metering_point] = await self._process_metering_point(
                    metering_point, selected_sensors
                )
        except UnauthorizedException as err:
            _LOGGER.error("Authentication error: %s", err)
            raise ConfigEntryAuthFailed("Invalid authentication") from err

        _LOGGER.debug("Completed data update for all metering points")
        return data

    async def _process_metering_point(
        self, metering_point: str, selected_sensors: list[str]
    ) -> dict[str, Any]:
        """Process a single metering point and its sensors.

        Args:
            metering_point: The metering point to process
            selected_sensors: List of sensor types to process

        Returns:
            Dictionary containing the meter data

        """
        _LOGGER.debug("Processing metering point: %s", metering_point)
        meter_data: dict[str, Any] = {"values": {}}

        for sensor_type in selected_sensors:
            cfg = SENSOR_TYPES.get(sensor_type)
            if not cfg:
                _LOGGER.error(
                    "Unknown sensor type %s for %s",
                    sensor_type,
                    metering_point,
                )
                continue

            obis = cfg["obis_code"]
            await self._update_statistics(metering_point, obis)
            current_total = await self._get_current_total(metering_point, obis)
            meter_data["values"][obis] = current_total

        return meter_data

    async def _update_statistics(self, metering_point: str, obis: ObisCode) -> None:
        """Update statistics for a metering point and OBIS code.

        Args:
            metering_point: The metering point to update
            obis: The OBIS code to update

        """
        statistic_id = _create_statistic_id(metering_point, obis)
        start_date = await self._get_statistics_start_date(statistic_id)
        end_date = datetime.now().astimezone(timezone.utc)

        # No need to measure anything if there will be no new data
        if end_date - timedelta(days=1) < start_date:
            return

        # This should be really just 1 hour at most
        start_date = start_date - timedelta(days=1)

        result = await self._fetch_hourly_data(
            metering_point, obis, start_date, end_date
        )
        if not result.aggregated_time_series:
            return

        await self._process_and_store_statistics(
            statistic_id, metering_point, obis, result.aggregated_time_series
        )

    async def _get_statistics_start_date(self, statistic_id: str) -> datetime:
        """Get the start date for statistics update.

        Args:
            statistic_id: The statistic ID to check

        Returns:
            The start date for fetching statistics

        """
        last_stat = await get_instance(self.hass).async_add_executor_job(
            get_last_statistics, self.hass, 1, statistic_id, True, set()
        )

        if not last_stat:
            # This should be taken from the statistics stored, but right now does not seem possible
            return STATISTICS_PERIOD_START

        start_date = dt_util.utc_from_timestamp(last_stat[statistic_id][0]["end"])
        return start_date

    async def _fetch_hourly_data(
        self,
        metering_point: str,
        obis: ObisCode,
        start_date: datetime,
        end_date: datetime,
    ):
        """Fetch hourly aggregated data from the API.

        Args:
            metering_point: The metering point to fetch data for
            obis: The OBIS code to fetch data for
            start_date: Start date for the data fetch
            end_date: End date for the data fetch

        Returns:
            The API response containing aggregated time series data

        """
        _LOGGER.debug(
            "Fetching hourly data for %s from %s to %s",
            _create_statistic_id(metering_point, obis),
            start_date,
            end_date,
        )

        result = await self.client.get_aggregated_metering_data(
            metering_point,
            obis,
            start_date,
            end_date,
            "Hour",
            "Accumulation",
        )

        _LOGGER.debug(
            "Successfully fetched hourly data, %d values found",
            len(result.aggregated_time_series),
        )
        return result

    async def _process_and_store_statistics(
        self,
        statistic_id: str,
        metering_point: str,
        obis: str,
        time_series: list,
    ) -> None:
        """Process time series data and store statistics.

        Args:
            statistic_id: The statistic ID to store data for
            metering_point: The metering point identifier
            obis: The OBIS code
            time_series: List of time series data points

        """
        stats = await self._get_existing_statistics(statistic_id)
        statistics = await self._prepare_statistics(statistic_id, time_series, stats)

        if statistics:
            await self._store_statistics(statistic_id, metering_point, obis, statistics)

    async def _get_existing_statistics(self, statistic_id: str) -> dict:
        """Get existing statistics for a given ID.

        Args:
            statistic_id: The statistic ID to fetch data for

        Returns:
            Dictionary containing existing statistics

        """
        # This shouldn't need to take all statistics
        return await get_instance(self.hass).async_add_executor_job(
            statistics_during_period,
            self.hass,
            STATISTICS_PERIOD_START,
            None,
            {statistic_id},
            "hour",
            None,
            {"sum"},
        )

    async def _prepare_statistics(
        self, statistic_id: str, time_series: list, existing_stats: dict
    ) -> list[StatisticData]:
        """Prepare statistics data for storage.

        Args:
            statistic_id: The statistic ID
            time_series: List of time series data points
            existing_stats: Dictionary of existing statistics

        Returns:
            List of prepared StatisticData objects

        """
        last_stats_time = (
            existing_stats[statistic_id][0]["end"]
            if existing_stats and statistic_id in existing_stats
            else None
        )

        last_sum = (
            float(cast(float, existing_stats[statistic_id][0]["sum"]))
            if existing_stats
            and statistic_id in existing_stats
            and existing_stats[statistic_id][0]["sum"] is not None
            else 0.0
        )

        _LOGGER.debug(f"_prepare_statistics: {last_stats_time} {last_sum}")

        statistics = []
        for point in time_series:
            if (
                last_stats_time is not None
                and point.started_at.timestamp() <= last_stats_time
            ):
                continue

            value = float(point.value)
            last_sum += value
            _LOGGER.debug(
                f"_prepare_statistics: {point.started_at.timestamp()} {point.started_at} {last_sum} {value}"
            )
            statistics.append(
                StatisticData(
                    start=point.started_at,
                    state=value,
                    sum=last_sum,
                )
            )

        return statistics

    async def _store_statistics(
        self,
        statistic_id: str,
        metering_point: str,
        obis: str,
        statistics: list[StatisticData],
    ) -> None:
        """Store statistics in Home Assistant.

        Args:
            statistic_id: The statistic ID
            metering_point: The metering point identifier
            obis: The OBIS code
            statistics: List of statistics to store

        """
        obis_info = get_obis_info(obis)
        unit_of_measurement = UNIT_TO_AGGREGATED_UNIT.get(
            obis_info.unit.lower(), obis_info.unit
        )

        async_add_external_statistics(
            self.hass,
            StatisticMetaData(
                mean_type=StatisticMeanType.NONE,
                has_sum=True,
                name=f"{metering_point} {obis}",
                source=DOMAIN,
                statistic_id=statistic_id,
                unit_of_measurement=unit_of_measurement,
            ),
            statistics,
        )
        _LOGGER.debug("Successfully added statistics for %s", statistic_id)

    async def _get_current_total(
        self, metering_point: str, obis: ObisCode
    ) -> float | None:
        """Get current total consumption for a metering point and OBIS code.

        Args:
            metering_point: The metering point to get data for
            obis: The OBIS code to get data for

        Returns:
            The current total consumption or None if no data available

        """
        current_year = datetime.now().year
        start_date = datetime(current_year, 1, 1)
        end_date = datetime.now()

        result = await self.client.get_aggregated_metering_data(
            metering_point,
            obis,
            start_date,
            end_date,
            "Infinite",
            "Accumulation",
        )

        if not result.aggregated_time_series:
            return None

        total = sum(float(pt.value) for pt in result.aggregated_time_series)
        _LOGGER.debug("Current total for %s %s: %s", metering_point, obis, total)
        return total
