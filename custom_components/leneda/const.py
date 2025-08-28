"""Constants for the Leneda integration."""

from __future__ import annotations

from datetime import timedelta, datetime, timezone
from typing import TypedDict

from leneda.obis_codes import ObisCode

from homeassistant.components.sensor import SensorDeviceClass, SensorStateClass
from homeassistant.const import (
    UnitOfEnergy,
    UnitOfPower,
    UnitOfReactiveEnergy,
    UnitOfReactivePower,
)

DOMAIN = "leneda"

CONF_API_TOKEN = "api_token"
CONF_ENERGY_ID = "energy_id"
CONF_METERING_POINT = "metering_point"

SCAN_INTERVAL = timedelta(hours=1)

# For now defaulting to 20 years, which should be good enough for everyone
STATISTICS_PERIOD_START = datetime(2000, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


class SensorTypeConfig(TypedDict):
    """Configuration for a sensor type."""

    obis_code: ObisCode
    device_class: SensorDeviceClass | None
    state_class: SensorStateClass


# Sensor types and their corresponding OBIS codes
SENSOR_TYPES: dict[str, SensorTypeConfig] = {
    # Electricity Consumption
    "electricity_consumption_active": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_ACTIVE,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_reactive": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_REACTIVE,
        "device_class": SensorDeviceClass.REACTIVE_ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_covered_layer1": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_COVERED_LAYER1,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_covered_layer2": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_COVERED_LAYER2,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_covered_layer3": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_COVERED_LAYER3,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_covered_layer4": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_COVERED_LAYER4,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_consumption_remaining": {
        "obis_code": ObisCode.ELEC_CONSUMPTION_REMAINING,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    # Electricity Production
    "electricity_production_active": {
        "obis_code": ObisCode.ELEC_PRODUCTION_ACTIVE,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_reactive": {
        "obis_code": ObisCode.ELEC_PRODUCTION_REACTIVE,
        "device_class": SensorDeviceClass.REACTIVE_ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_shared_layer1": {
        "obis_code": ObisCode.ELEC_PRODUCTION_SHARED_LAYER1,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_shared_layer2": {
        "obis_code": ObisCode.ELEC_PRODUCTION_SHARED_LAYER2,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_shared_layer3": {
        "obis_code": ObisCode.ELEC_PRODUCTION_SHARED_LAYER3,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_shared_layer4": {
        "obis_code": ObisCode.ELEC_PRODUCTION_SHARED_LAYER4,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "electricity_production_remaining": {
        "obis_code": ObisCode.ELEC_PRODUCTION_REMAINING,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    # Gas Consumption
    "gas_consumption_volume": {
        "obis_code": ObisCode.GAS_CONSUMPTION_VOLUME,
        "device_class": SensorDeviceClass.GAS,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "gas_consumption_standard_volume": {
        "obis_code": ObisCode.GAS_CONSUMPTION_STANDARD_VOLUME,
        "device_class": None,  # Ideally SensorDeviceClass.GAS, but Nm3 not supported by Hass
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
    "gas_consumption_energy": {
        "obis_code": ObisCode.GAS_CONSUMPTION_ENERGY,
        "device_class": SensorDeviceClass.ENERGY,
        "state_class": SensorStateClass.TOTAL_INCREASING,
    },
}

UNIT_TO_AGGREGATED_UNIT = {
    UnitOfPower.KILO_WATT.lower(): UnitOfEnergy.KILO_WATT_HOUR,
    UnitOfReactivePower.KILO_VOLT_AMPERE_REACTIVE.lower(): UnitOfReactiveEnergy.KILO_VOLT_AMPERE_REACTIVE_HOUR,
}
