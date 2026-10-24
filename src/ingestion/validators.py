REQUIRED_FIELDS = {
    "anio",
    "vehiculo",
    "siniestros",
    "fallecidos",
    "graves",
    "menos_graves",
    "leves"
}

NUMERIC_FIELDS = {
    "siniestros",
    "fallecidos",
    "graves",
    "menos_graves",
    "leves"
}

def validate_records(records: list[dict]) -> None:

    if not records:
        raise ValueError("The dataset is empty")

    seen_keys = set()

    for index, record in enumerate(records, start=1):

        # 1. Validar estructura
        if not isinstance(record, dict):
            raise ValueError(
                f"Record {index} is not a dictionary"
            )

        # 2. Validar columnas obligatorias
        missing_fields = REQUIRED_FIELDS - record.keys()

        if missing_fields:
            raise ValueError(
                f"Record {index}: missing fields {missing_fields}"
            )

        # 3. Validar año
        year = record["anio"]

        if type(year) is not int or year <= 0:
            raise ValueError(
                f"Record {index}: invalid year {year}"
            )

        # 4. Validar vehículo
        vehicle = record["vehiculo"]

        if not isinstance(vehicle, str) or not vehicle.strip():
            raise ValueError(
                f"Record {index}: invalid vehicle"
            )

        # 5. Validar campos numéricos
        for field in NUMERIC_FIELDS:

            value = record[field]

            if type(value) is not int or value < 0:
                raise ValueError(
                    f"Record {index}: invalid value "
                    f"for {field}: {value}"
                )

        # 6. Validar clave única
        key = (year, vehicle.strip().casefold())

        if key in seen_keys:
            raise ValueError(
                f"Record {index}: duplicate key {key}"
            )

        seen_keys.add(key)