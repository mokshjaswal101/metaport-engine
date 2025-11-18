"""
Courier Buy Rates Configuration
This file contains buy rates for different aggregators and their couriers
to calculate tentative PNL against sell rates.

Structure:
aggregator: {
    courier: {
        rates configuration including zones, weights, cod charges etc.
    }
}
"""

from decimal import Decimal

# Courier Buy Rates Configuration
COURIER_BUY_RATES = {
    "shiperfecto": {
        "bluedart": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("25.00"),
                "zone_b": Decimal("31.00"),
                "zone_c": Decimal("37.00"),
                "zone_d": Decimal("43.00"),
                "zone_e": Decimal("52.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("25.00"),
                "zone_b": Decimal("31.00"),
                "zone_c": Decimal("37.00"),
                "zone_d": Decimal("43.00"),
                "zone_e": Decimal("52.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("25.00"),
                "zone_b": Decimal("31.00"),
                "zone_c": Decimal("37.00"),
                "zone_d": Decimal("43.00"),
                "zone_e": Decimal("52.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("25.00"),
                "zone_b": Decimal("31.00"),
                "zone_c": Decimal("37.00"),
                "zone_d": Decimal("43.00"),
                "zone_e": Decimal("52.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("30.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "bluedart-air": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("37.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("39.00"),
                "zone_c": Decimal("45.00"),
                "zone_d": Decimal("51.00"),
                "zone_e": Decimal("61.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("37.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("39.00"),
                "zone_c": Decimal("45.00"),
                "zone_d": Decimal("51.00"),
                "zone_e": Decimal("61.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("37.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("39.00"),
                "zone_c": Decimal("45.00"),
                "zone_d": Decimal("51.00"),
                "zone_e": Decimal("61.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("37.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("39.00"),
                "zone_c": Decimal("45.00"),
                "zone_d": Decimal("51.00"),
                "zone_e": Decimal("61.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.2"),
                "absolute_rate": Decimal("32.00"),
            },
            "tax_rate": Decimal("18.0"),  # Special hyperlocal charge
        },
        "dtdc": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("28.00"),
                "zone_b": Decimal("34.00"),
                "zone_c": Decimal("40.00"),
                "zone_d": Decimal("46.00"),
                "zone_e": Decimal("55.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("28.00"),
                "zone_b": Decimal("34.00"),
                "zone_c": Decimal("40.00"),
                "zone_d": Decimal("46.00"),
                "zone_e": Decimal("55.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("28.00"),
                "zone_b": Decimal("34.00"),
                "zone_c": Decimal("40.00"),
                "zone_d": Decimal("46.00"),
                "zone_e": Decimal("55.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("28.00"),
                "zone_b": Decimal("34.00"),
                "zone_c": Decimal("40.00"),
                "zone_d": Decimal("46.00"),
                "zone_e": Decimal("55.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.8"),
                "absolute_rate": Decimal("35.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
    "delhivery": {
        "delhivery": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("23.00"),
                "zone_b": Decimal("26.00"),
                "zone_c": Decimal("33.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("50.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("21.00"),
                "zone_b": Decimal("23.00"),
                "zone_c": Decimal("30.00"),
                "zone_d": Decimal("33.00"),
                "zone_e": Decimal("45.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("19.00"),
                "zone_b": Decimal("22.00"),
                "zone_c": Decimal("27.00"),
                "zone_d": Decimal("30.00"),
                "zone_e": Decimal("40.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("17.00"),
                "zone_b": Decimal("19.00"),
                "zone_c": Decimal("24.00"),
                "zone_d": Decimal("28.00"),
                "zone_e": Decimal("36.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.2"),
                "absolute_rate": Decimal("25.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "delhivery-air": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("26.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("30.00"),
                "zone_c": Decimal("42.00"),
                "zone_d": Decimal("48.00"),
                "zone_e": Decimal("62.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("24.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("29.00"),
                "zone_c": Decimal("42.00"),
                "zone_d": Decimal("48.00"),
                "zone_e": Decimal("62.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("23.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("27.00"),
                "zone_c": Decimal("42.00"),
                "zone_d": Decimal("48.00"),
                "zone_e": Decimal("62.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("23.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("27.00"),
                "zone_c": Decimal("42.00"),
                "zone_d": Decimal("48.00"),
                "zone_e": Decimal("62.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("25.00"),
            },
            "tax_rate": Decimal("18.0"),  # Special hyperlocal charge
            "fuel_surcharge": Decimal("5.0"),
        },
        "delhivery 2kg": {
            "min_chargeable_weight": Decimal("2.0"),
            "additional_weight_bracket": Decimal("1.0"),
            "base_rates": {
                "zone_a": Decimal("65.00"),
                "zone_b": Decimal("72.00"),
                "zone_c": Decimal("80.00"),
                "zone_d": Decimal("86.00"),
                "zone_e": Decimal("105.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("29.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("36.00"),
                "zone_d": Decimal("41.00"),
                "zone_e": Decimal("47.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("65.00"),
                "zone_b": Decimal("72.00"),
                "zone_c": Decimal("80.00"),
                "zone_d": Decimal("86.00"),
                "zone_e": Decimal("105.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("29.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("36.00"),
                "zone_d": Decimal("40.00"),
                "zone_e": Decimal("47.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("26.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "delhivery 5kg": {
            "min_chargeable_weight": Decimal("5.0"),
            "additional_weight_bracket": Decimal("1.0"),
            "base_rates": {
                "zone_a": Decimal("121.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("132.00"),
                "zone_c": Decimal("142.00"),
                "zone_d": Decimal("158.00"),
                "zone_e": Decimal("197.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("23.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("24.00"),
                "zone_c": Decimal("26.00"),
                "zone_d": Decimal("30.00"),
                "zone_e": Decimal("35.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("109.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("119.00"),
                "zone_c": Decimal("128.00"),
                "zone_d": Decimal("142.00"),
                "zone_e": Decimal("177.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("21.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("22.00"),
                "zone_c": Decimal("23.00"),
                "zone_d": Decimal("27.00"),
                "zone_e": Decimal("32.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("26.00"),
            },
            "tax_rate": Decimal("18.0"),  # Special hyperlocal charge
        },
        "delhivery 10kg": {
            "min_chargeable_weight": Decimal("10"),
            "additional_weight_bracket": Decimal("1"),
            "base_rates": {
                "zone_a": Decimal("184.00"),
                "zone_b": Decimal("230.00"),
                "zone_c": Decimal("266.00"),
                "zone_d": Decimal("308.00"),
                "zone_e": Decimal("389.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("17.00"),
                "zone_b": Decimal("21.00"),
                "zone_c": Decimal("24.00"),
                "zone_d": Decimal("28.00"),
                "zone_e": Decimal("35.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("184.00"),
                "zone_b": Decimal("230.00"),
                "zone_c": Decimal("266.00"),
                "zone_d": Decimal("308.00"),
                "zone_e": Decimal("389.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("17.00"),
                "zone_b": Decimal("21.00"),
                "zone_c": Decimal("24.00"),
                "zone_d": Decimal("28.00"),
                "zone_e": Decimal("35.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("28.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "delhivery 20kg": {
            "min_chargeable_weight": Decimal("20"),
            "additional_weight_bracket": Decimal("1"),
            "base_rates": {
                "zone_a": Decimal("295.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("365.00"),
                "zone_c": Decimal("410.00"),
                "zone_d": Decimal("480.00"),
                "zone_e": Decimal("651.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("14.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("17.00"),
                "zone_c": Decimal("18.00"),
                "zone_d": Decimal("22.00"),
                "zone_e": Decimal("30.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("295.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("365.00"),
                "zone_c": Decimal("410.00"),
                "zone_d": Decimal("480.00"),
                "zone_e": Decimal("651.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("14.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("17.00"),
                "zone_c": Decimal("18.00"),
                "zone_d": Decimal("22.00"),
                "zone_e": Decimal("30.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("28.00"),
            },
            "tax_rate": Decimal("18.0"),  # Special hyperlocal charge
        },
    },
    "dtdc": {
        "dtdc": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("24.00"),
                "zone_b": Decimal("26.00"),
                "zone_c": Decimal("29.00"),
                "zone_d": Decimal("32.00"),
                "zone_e": Decimal("46.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("13.00"),
                "zone_b": Decimal("15.00"),
                "zone_c": Decimal("17.00"),
                "zone_d": Decimal("21.00"),
                "zone_e": Decimal("30.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("24.00"),
                "zone_b": Decimal("26.00"),
                "zone_c": Decimal("29.00"),
                "zone_d": Decimal("32.00"),
                "zone_e": Decimal("46.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("13.00"),
                "zone_b": Decimal("15.00"),
                "zone_c": Decimal("17.00"),
                "zone_d": Decimal("21.00"),
                "zone_e": Decimal("30.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.0"),
                "absolute_rate": Decimal("20.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "dtdc 3kg": {
            "min_chargeable_weight": Decimal("3"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("31.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("43.00"),
                "zone_c": Decimal("52.00"),
                "zone_d": Decimal("61.00"),
                "zone_e": Decimal("76.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("09.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("13.00"),
                "zone_c": Decimal("16.00"),
                "zone_d": Decimal("19.00"),
                "zone_e": Decimal("24.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("31.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("43.00"),
                "zone_c": Decimal("52.00"),
                "zone_d": Decimal("61.00"),
                "zone_e": Decimal("76.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("09.00"),  # Shadowfax is typically hyperlocal
                "zone_b": Decimal("13.00"),
                "zone_c": Decimal("16.00"),
                "zone_d": Decimal("19.00"),
                "zone_e": Decimal("24.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1"),
                "absolute_rate": Decimal("20.00"),
            },
            "tax_rate": Decimal("18.0"),  # Spe
        },
        "dtdc-air": {
            "min_chargeable_weight": Decimal("2.0"),
            "additional_weight_bracket": Decimal("1.0"),
            "base_rates": {
                "zone_a": Decimal("23.00"),
                "zone_b": Decimal("25.00"),
                "zone_c": Decimal("38.00"),
                "zone_d": Decimal("42.00"),
                "zone_e": Decimal("56.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("16.00"),
                "zone_b": Decimal("20.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("38.00"),
                "zone_e": Decimal("50.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("23.00"),
                "zone_b": Decimal("25.00"),
                "zone_c": Decimal("38.00"),
                "zone_d": Decimal("42.00"),
                "zone_e": Decimal("56.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("16.00"),
                "zone_b": Decimal("20.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("38.00"),
                "zone_e": Decimal("50.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1"),
                "absolute_rate": Decimal("20.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
    "shadowfax": {
        "shadowfax": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("22.00"),
                "zone_b": Decimal("24.00"),
                "zone_c": Decimal("28.00"),
                "zone_d": Decimal("32.00"),
                "zone_e": Decimal("48.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("18.00"),
                "zone_b": Decimal("20.00"),
                "zone_c": Decimal("22.00"),
                "zone_d": Decimal("25.00"),
                "zone_e": Decimal("37.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("17.60"),  # 80% of 22.00
                "zone_b": Decimal("19.20"),  # 80% of 24.00
                "zone_c": Decimal("22.40"),  # 80% of 28.00
                "zone_d": Decimal("25.60"),  # 80% of 32.00
                "zone_e": Decimal("38.40"),  # 80% of 48.00
            },
            "rto_additional_rates": {
                "zone_a": Decimal("14.40"),  # 80% of 18.00
                "zone_b": Decimal("16.00"),  # 80% of 20.00
                "zone_c": Decimal("17.60"),  # 80% of 22.00
                "zone_d": Decimal("20.00"),  # 80% of 25.00
                "zone_e": Decimal("29.60"),  # 80% of 37.00
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.0"),
                "absolute_rate": Decimal("20.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
    "shiprocket": {
        "bluedart": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("30.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("55.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("30.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("55.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("30.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("55.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("30.00"),
                "zone_b": Decimal("32.00"),
                "zone_c": Decimal("34.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("55.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.4"),
                "absolute_rate": Decimal("24.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "bluedart 2kg": {
            "min_chargeable_weight": Decimal("2"),
            "additional_weight_bracket": Decimal("1"),
            "base_rates": {
                "zone_a": Decimal("110.00"),
                "zone_b": Decimal("110.00"),
                "zone_c": Decimal("110.00"),
                "zone_d": Decimal("110.00"),
                "zone_e": Decimal("110.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("40.00"),
                "zone_b": Decimal("40.00"),
                "zone_c": Decimal("40.00"),
                "zone_d": Decimal("40.00"),
                "zone_e": Decimal("40.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("0"),  # 80% of 22.00
                "zone_b": Decimal("0"),  # 80% of 24.00
                "zone_c": Decimal("0"),  # 80% of 28.00
                "zone_d": Decimal("0"),  # 80% of 32.00
                "zone_e": Decimal("0"),  # 80% of 48.00
            },
            "rto_additional_rates": {
                "zone_a": Decimal("0"),  # 80% of 22.00
                "zone_b": Decimal("0"),  # 80% of 24.00
                "zone_c": Decimal("0"),  # 80% of 28.00
                "zone_d": Decimal("0"),  # 80% of 32.00
                "zone_e": Decimal("0"),  # 80% of 48.00
            },
            "cod_charges": {
                "percentage_rate": Decimal("0.0"),
                "absolute_rate": Decimal("0.00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
    "zippyy": {
        "ekart 1kg": {
            "min_chargeable_weight": Decimal("1"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("60.00"),
                "zone_b": Decimal("60.00"),
                "zone_c": Decimal("60.00"),
                "zone_d": Decimal("60.00"),
                "zone_e": Decimal("60.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("60.00"),
                "zone_b": Decimal("60.00"),
                "zone_c": Decimal("60.00"),
                "zone_d": Decimal("60.00"),
                "zone_e": Decimal("60.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("0.00"),
                "zone_b": Decimal("0.00"),
                "zone_c": Decimal("0.00"),
                "zone_d": Decimal("0.00"),
                "zone_e": Decimal("0.00"),
            },
            "rto_additional_rates": {
                "zone_a": Decimal("0.00"),
                "zone_b": Decimal("0.00"),
                "zone_c": Decimal("0.00"),
                "zone_d": Decimal("0.00"),
                "zone_e": Decimal("0.00"),
            },
            "cod_charges": {
                "percentage_rate": Decimal("0.0"),
                "absolute_rate": Decimal("0"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
    "xpressbees": {
        "xpressbees": {
            "min_chargeable_weight": Decimal("0.5"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("22.00"),
                "zone_b": Decimal("24.00"),
                "zone_c": Decimal("32.00"),
                "zone_d": Decimal("36.00"),
                "zone_e": Decimal("42.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("19.00"),
                "zone_b": Decimal("22.00"),
                "zone_c": Decimal("23.00"),
                "zone_d": Decimal("28.00"),
                "zone_e": Decimal("30.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("17.60"),  # 80% of 22.00
                "zone_b": Decimal("19.20"),  # 80% of 24.00
                "zone_c": Decimal("25.60"),  # 80% of 32.00
                "zone_d": Decimal("28.80"),  # 80% of 36.00
                "zone_e": Decimal("33.60"),  # 80% of 42.00
            },
            "rto_additional_rates": {
                "zone_a": Decimal("15.20"),  # 80% of 19.00
                "zone_b": Decimal("17.60"),  # 80% of 22.00
                "zone_c": Decimal("18.40"),  # 80% of 23.00
                "zone_d": Decimal("22.40"),  # 80% of 28.00
                "zone_e": Decimal("24.00"),  # 80% of 30.00
            },
            "cod_charges": {
                "percentage_rate": Decimal("1.5"),
                "absolute_rate": Decimal("21"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
        "xpressbees 1kg": {
            "min_chargeable_weight": Decimal("1"),
            "additional_weight_bracket": Decimal("0.5"),
            "base_rates": {
                "zone_a": Decimal("70.00"),
                "zone_b": Decimal("70.00"),
                "zone_c": Decimal("70.00"),
                "zone_d": Decimal("70.00"),
                "zone_e": Decimal("70.00"),
            },
            "additional_rates": {
                "zone_a": Decimal("70.00"),
                "zone_b": Decimal("70.00"),
                "zone_c": Decimal("70.00"),
                "zone_d": Decimal("70.00"),
                "zone_e": Decimal("70.00"),
            },
            "rto_base_rates": {
                "zone_a": Decimal("0"),  # 80% of 22.00
                "zone_b": Decimal("0"),  # 80% of 24.00
                "zone_c": Decimal("0"),  # 80% of 32.00
                "zone_d": Decimal("0"),  # 80% of 36.00
                "zone_e": Decimal("0"),  # 80% of 42.00
            },
            "rto_additional_rates": {
                "zone_a": Decimal("0"),  # 80% of 22.00
                "zone_b": Decimal("0"),  # 80% of 24.00
                "zone_c": Decimal("0"),  # 80% of 32.00
                "zone_d": Decimal("0"),  # 80% of 36.00
                "zone_e": Decimal("0"),  # 80% of 42.00
            },
            "cod_charges": {
                "percentage_rate": Decimal("0"),
                "absolute_rate": Decimal("00"),
            },
            "tax_rate": Decimal("18.0"),
            "fuel_surcharge": Decimal("0.0"),
            "handling_charge": Decimal("0.00"),
        },
    },
}


# Courier Partner Mapping - Maps specific courier partner names to base courier configurations
COURIER_PARTNER_MAPPING = {
    "dtdc": {
        # DTDC variants
        "dtdc": "dtdc",
        "dtdc 1kg": "dtdc",
        "dtdc 2kg": "dtdc",
        "dtdc 3kg": "dtdc 3kg",
        "dtdc 5kg": "dtdc 3kg",
        "dtdc 10kg": "dtdc 3kg",
        "dtdc-air": "dtdc-air",
        # Add more mappings as needed
    },
    "shadowfax": {
        "shadowfax": "shadowfax",
        "shadowfax 2kg": "shadowfax",
        "shadowfax 1kg": "shadowfax",
    },
    "zippyy": {
        "ekart 1kg": "ekart 1kg",
    },
    "shiprocket": {
        "bluedart": "bluedart",
        "bluedart 2kg": "bluedart 2kg",
    },
}


def normalize_courier_partner(aggregator: str, courier_partner: str) -> str:
    """
    Normalize courier partner name to match the configuration keys

    Args:
        aggregator: Aggregator name (e.g., 'shiperfecto')
        courier_partner: Raw courier partner name from order (e.g., 'dtdc 2kg', 'bluedart air')

    Returns:
        str: Normalized courier name that matches COURIER_BUY_RATES keys
    """
    if not courier_partner:
        return "bluedart"  # Default fallback

    # Clean the courier partner name
    courier_clean = courier_partner.lower().strip()

    # Get mapping for the aggregator
    aggregator_mapping = COURIER_PARTNER_MAPPING.get(aggregator.lower(), {})

    # Try exact match first
    if courier_clean in aggregator_mapping:
        return aggregator_mapping[courier_clean]

    # Try partial matching for common patterns
    for pattern, mapped_courier in aggregator_mapping.items():
        if pattern in courier_clean or courier_clean in pattern:
            return mapped_courier

    # If no mapping found, try to extract base courier name
    # Remove common suffixes like weight specifications, air/surface
    base_name = courier_clean
    for suffix in [
        " air",
        " surface",
        " 1kg",
        " 2kg",
        " 3kg",
        " 5kg",
        " 10kg",
        " express",
    ]:
        base_name = base_name.replace(suffix, "").strip()

    # Check if base name exists in mapping
    if base_name in aggregator_mapping:
        return aggregator_mapping[base_name]

    # Final fallback - return cleaned name or default
    return base_name if base_name else "bluedart"


def get_buy_rate(aggregator: str, courier: str, rate_type: str = "base_rates") -> dict:
    """
    Get buy rate configuration for a specific aggregator and courier

    Args:
        aggregator: Aggregator name (e.g., 'shiprocket', 'shiperfecto')
        courier: Courier name (e.g., 'bluedart', 'delhivery')
        rate_type: Type of rates to fetch ('base_rates', 'additional_rates', 'rto_base_rates', etc.)

    Returns:
        dict: Rate configuration or empty dict if not found
    """
    return COURIER_BUY_RATES.get(aggregator, {}).get(courier, {}).get(rate_type, {})


def get_courier_config(aggregator: str, courier: str) -> dict:
    """
    Get complete courier configuration including all rate types

    Args:
        aggregator: Aggregator name
        courier: Courier name (will be normalized automatically)

    Returns:
        dict: Complete courier configuration or empty dict if not found
    """
    # Normalize the courier name to match configuration keys
    normalized_courier = normalize_courier_partner(aggregator, courier)
    return COURIER_BUY_RATES.get(aggregator, {}).get(normalized_courier, {})


def calculate_buy_freight(
    aggregator: str,
    courier: str,
    zone: str,
    applicable_weight: float,
    order_value: float = 0,
    payment_mode: str = "prepaid",
    is_rto: bool = False,
) -> dict:
    """
    Calculate buy freight for given parameters

    Args:
        aggregator: Aggregator name
        courier: Courier name
        zone: Zone (A, B, C, D, E)
        applicable_weight: Weight in kg
        order_value: Order value for COD calculation
        payment_mode: 'cod' or 'prepaid'
        is_rto: Whether this is RTO calculation

    Returns:
        dict: Calculated freight breakdown
    """
    import math

    courier_config = get_courier_config(aggregator, courier)
    if not courier_config:
        return {
            "error": f"Configuration not found for {aggregator}/{courier}",
            "freight": 0,
            "cod_charges": 0,
            "tax_amount": 0,
            "total_amount": 0,
        }

    min_weight = float(courier_config["min_chargeable_weight"])
    bracket = float(courier_config["additional_weight_bracket"])

    # Calculate chargeable weight
    if applicable_weight < min_weight:
        chargeable_weight = min_weight
        additional_brackets = 0
    else:
        additional_weight = applicable_weight - min_weight
        additional_brackets = math.ceil(additional_weight / bracket)
        chargeable_weight = min_weight + (additional_brackets * bracket)

    # Get rates based on RTO or forward
    zone_key = f"zone_{zone.lower()}"

    if is_rto:
        # For RTO: Calculate both forward and RTO charges
        # Forward leg charges
        forward_base_rates = courier_config.get("base_rates", {})
        forward_additional_rates = courier_config.get("additional_rates", {})
        forward_base_freight = float(forward_base_rates.get(zone_key, 0))
        forward_additional_freight = (
            float(forward_additional_rates.get(zone_key, 0)) * additional_brackets
        )
        forward_freight = forward_base_freight + forward_additional_freight

        # RTO leg charges
        rto_base_rates = courier_config.get("rto_base_rates", {})
        rto_additional_rates = courier_config.get("rto_additional_rates", {})
        rto_base_freight = float(rto_base_rates.get(zone_key, 0))
        rto_additional_freight = (
            float(rto_additional_rates.get(zone_key, 0)) * additional_brackets
        )
        rto_freight = rto_base_freight + rto_additional_freight

        # Total freight for RTO = Forward + RTO charges
        freight = forward_freight + rto_freight

        base_rates = rto_base_rates  # For reference in return object
        additional_rates = rto_additional_rates
    else:
        # For forward shipments: Only forward charges
        base_rates = courier_config.get("base_rates", {})
        additional_rates = courier_config.get("additional_rates", {})
        base_freight = float(base_rates.get(zone_key, 0))
        additional_freight = (
            float(additional_rates.get(zone_key, 0)) * additional_brackets
        )
        freight = base_freight + additional_freight

    # Calculate COD charges
    cod_charges = 0
    if payment_mode.lower() == "cod" and not is_rto:
        # COD charges only apply for forward deliveries, not for RTO
        cod_config = courier_config.get("cod_charges", {})
        percentage_rate = float(cod_config.get("percentage_rate", 0))
        absolute_rate = float(cod_config.get("absolute_rate", 0))

        percentage_charge = (order_value * percentage_rate) / 100
        cod_charges = max(absolute_rate, percentage_charge)
    elif payment_mode.lower() == "cod" and is_rto:
        # For RTO on COD orders, subtract the COD charges that were paid on forward leg
        cod_config = courier_config.get("cod_charges", {})
        percentage_rate = float(cod_config.get("percentage_rate", 0))
        absolute_rate = float(cod_config.get("absolute_rate", 0))

        percentage_charge = (order_value * percentage_rate) / 100
        cod_charges = -max(
            absolute_rate, percentage_charge
        )  # Negative because it's a refund

    # Add additional charges
    fuel_surcharge = freight * float(courier_config.get("fuel_surcharge", 0)) / 100
    handling_charge = float(courier_config.get("handling_charge", 0))

    subtotal = freight + cod_charges + fuel_surcharge + handling_charge

    # Calculate tax
    tax_rate = float(courier_config.get("tax_rate", 18))
    tax_amount = (subtotal * tax_rate) / 100

    total_amount = subtotal + tax_amount

    result = {
        "freight": round(freight, 2),
        "cod_charges": round(cod_charges, 2),
        "tax_amount": round(tax_amount, 2),
        "fuel_surcharge": round(fuel_surcharge, 2),
        "handling_charge": round(handling_charge, 2),
        "subtotal": round(subtotal, 2),
        "total_amount": round(total_amount, 2),
        "chargeable_weight": chargeable_weight,
        "additional_brackets": additional_brackets,
        "is_rto": is_rto,
    }

    # Add detailed breakdown for RTO cases
    if is_rto:
        result.update(
            {
                "forward_freight": round(forward_freight, 2),
                "rto_freight": round(rto_freight, 2),
                "cod_refund": round(cod_charges, 2) if cod_charges < 0 else 0,
            }
        )

    return result
