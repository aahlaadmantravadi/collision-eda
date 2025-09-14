from sqlalchemy.types import Integer, Date, Time, Text

# URLs for datasets
url_C = "https://data.cityofnewyork.us/api/views/h9gi-nx95/rows.csv?accessType=DOWNLOAD"
url_V = "https://data.cityofnewyork.us/api/views/bm4k-52h4/rows.csv?accessType=DOWNLOAD"
url_P = "https://data.cityofnewyork.us/api/views/f55k-p6yu/rows.csv?accessType=DOWNLOAD"

# --- Schema and Column Definitions for 'Crashes' (C) ---
sel_C = [
    "COLLISION_ID", "CRASH DATE", "CRASH TIME", "BOROUGH",
    "NUMBER OF PERSONS INJURED", "NUMBER OF PERSONS KILLED",
    "VEHICLE TYPE CODE 1", "CONTRIBUTING FACTOR VEHICLE 1",
    "VEHICLE TYPE CODE 2", "CONTRIBUTING FACTOR VEHICLE 2",
    "VEHICLE TYPE CODE 3", "CONTRIBUTING FACTOR VEHICLE 3",
    "VEHICLE TYPE CODE 4", "CONTRIBUTING FACTOR VEHICLE 4"
]

sel_rename_C = {
    "COLLISION_ID": "collision_id", "CRASH DATE": "crash_date", "CRASH TIME": "crash_time",
    "BOROUGH": "borough", "NUMBER OF PERSONS INJURED": "injured", "NUMBER OF PERSONS KILLED": "killed",
    "CONTRIBUTING FACTOR VEHICLE 1": "contr_f_vhc_1", "VEHICLE TYPE CODE 1": "vhc_1_code",
    "CONTRIBUTING FACTOR VEHICLE 2": "contr_f_vhc_2", "VEHICLE TYPE CODE 2": "vhc_2_code",
    "CONTRIBUTING FACTOR VEHICLE 3": "contr_f_vhc_3", "VEHICLE TYPE CODE 3": "vhc_3_code",
    "CONTRIBUTING FACTOR VEHICLE 4": "contr_f_vhc_4", "VEHICLE TYPE CODE 4": "vhc_4_code"
}

sel_types_C = {
    "collision_id": Integer(), "crash_date": Date(), "crash_time": Time(), "borough": Text(),
    "injured": Integer(), "killed": Integer(), "vhc_1_code": Text(), "contr_f_vhc_1": Text(),
    "vhc_2_code": Text(), "contr_f_vhc_2": Text(), "vhc_3_code": Text(), "contr_f_vhc_3": Text(),
    "vhc_4_code": Text(), "contr_f_vhc_4": Text()
}

# --- Schema and Column Definitions for 'Vehicles' (V) ---
sel_V = [
    "UNIQUE_ID", "COLLISION_ID", "CRASH_DATE", "CRASH_TIME", "VEHICLE_TYPE", "VEHICLE_DAMAGE",
    "DRIVER_SEX", "DRIVER_LICENSE_STATUS", "VEHICLE_YEAR", "VEHICLE_OCCUPANTS",
    "STATE_REGISTRATION", "CONTRIBUTING_FACTOR_1"
]

sel_rename_V = {
    "UNIQUE_ID": "unique_id", "COLLISION_ID": "collision_id", "CRASH_DATE": "crash_date",
    "CRASH_TIME": "crash_time", "STATE_REGISTRATION": "state_reg", "VEHICLE_TYPE": "vhc_type",
    "VEHICLE_YEAR": "vhc_year", "VEHICLE_OCCUPANTS": "vhc_occupants", "DRIVER_SEX": "dr_sex",
    "DRIVER_LICENSE_STATUS": "dr_lic_status", "VEHICLE_DAMAGE": "vhc_dmg", "CONTRIBUTING_FACTOR_1": "contr_f"
}

sel_types_V = {
    "unique_id": Integer(), "collision_id": Integer(), "crash_date": Date(), "crash_time": Time(),
    "vhc_type": Text(), "vhc_dmg": Text(), "dr_sex": Text(), "dr_lic_status": Text(),
    "vhc_year": Integer(), "vhc_occupants": Integer(), "state_reg": Text(), "contr_f": Text()
}

# --- Schema and Column Definitions for 'Person' (P) ---
sel_P = [
    "UNIQUE_ID", "COLLISION_ID", "CRASH_DATE", "CRASH_TIME", "EJECTION", "BODILY_INJURY",
    "PERSON_INJURY", "POSITION_IN_VEHICLE", "SAFETY_EQUIPMENT", "PERSON_TYPE",
    "PERSON_AGE", "PERSON_SEX", "EMOTIONAL_STATUS", "CONTRIBUTING_FACTOR_1"
]

sel_rename_P = {
    "UNIQUE_ID": "unique_id", "COLLISION_ID": "collision_id", "CRASH_DATE": "crash_date",
    "CRASH_TIME": "crash_time", "EJECTION": "ejection", "BODILY_INJURY": "body_inj",
    "PERSON_INJURY": "person_inj", "POSITION_IN_VEHICLE": "pos_in_vhc",
    "SAFETY_EQUIPMENT": "safety_equip", "PERSON_TYPE": "person_type", "PERSON_AGE": "age",
    "PERSON_SEX": "sex", "EMOTIONAL_STATUS": "emot_status", "CONTRIBUTING_FACTOR_1": "contr_f"
}

sel_types_P = {
    "unique_id": Integer(), "collision_id": Integer(), "crash_date": Date(), "crash_time": Time(),
    "ejection": Text(), "body_inj": Text(), "person_inj": Text(), "pos_in_vhc": Text(),
    "safety_equip": Text(), "person_type": Text(), "age": Integer(), "sex": Text(),
    "emot_status": Text(), "contr_f": Text()
}


# --- Metabase Card Definitions ---
CARD_DEFINITIONS = [
    {
        'name': 'Total Collisions, Vehicles, and Persons (2012-2023)',
        'display': 'scalar',
        'dataset_query': {
            'type': 'native',
            'native': {
                'query': 'SELECT SUM(total_crashes_am) AS "Total Crashes", SUM(total_vhc_am) AS "Total Vehicles", SUM(total_person_am) AS "Total Persons" FROM "MVC_summarize".mvc_dataset_overview;'
            }
        }
    },
    {
        'name': 'Vehicle Types Involved in Crashes',
        'display': 'pie',
        'dataset_query': {
            'type': 'native',
            'native': {
                'query': "SELECT CASE WHEN lower(vhc_type) LIKE 'passenger vehicle' THEN 'Passenger Vehicle' WHEN lower(vhc_type) LIKE 'station wagon/sport utility vehicle' THEN 'SUV/Station Wagon' ELSE 'Other' END AS vehicle_category, SUM(vhc_type_am) as amount FROM \"MVC_summarize\".mvc_sum_all WHERE vhc_type IS NOT NULL GROUP BY vehicle_category ORDER BY amount DESC;"
            }
        },
        'visualization_settings': {'pie.slice_threshold': 0.05}
    },
    {
        'name': 'Top Contributing Factors to Collisions',
        'display': 'bar',
        'dataset_query': {
            'type': 'native',
            'native': {
                'query': 'SELECT contr_f AS "Contributing Factor", SUM(contr_f_am) AS amount FROM "MVC_summarize".mvc_sum_all WHERE contr_f IS NOT NULL GROUP BY contr_f ORDER BY amount DESC LIMIT 10;'
            }
        }
    },
    {
        'name': 'Collisions by Month (2016-2022)',
        'display': 'line',
        'dataset_query': {
            'type': 'native',
            'native': {
                'query': 'SELECT concat("year",\'-\',TO_CHAR("month",\'fm09\')) AS "Date", SUM(all_amount) AS "Total Crashes", SUM(injured_am) AS "Crashes with Injuries" FROM "MVC_summarize".mvc_crashes_per_hour WHERE "year" BETWEEN 2016 AND 2022 GROUP BY "Date" ORDER BY "Date";'
            }
        }
    }
]