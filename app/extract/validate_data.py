from config.logging_config import logger
from datetime import datetime, timezone

def validate_weather_data(city, data):
    """Validate weather data and log invalid fields."""
    invalid_fields = []

    required_keys = ['dt', 'main', 'wind', 'sys', 'name']
    for key in required_keys:
        if key not in data:
            invalid_fields.append({'field': key, 'value': data.get(key)})

    # Validate datetime field
    if 'dt' not in data or not isinstance(data['dt'], int):
        invalid_fields.append({'field': 'dt', 'value': data.get('dt')})
    else:
        try:
            # Validate that 'dt' can be converted to a valid UTC datetime
            datetime.fromtimestamp(data['dt'], tz=timezone.utc)
        except (ValueError, OverflowError):
            invalid_fields.append({'field': 'dt', 'value': data.get('dt')})

    # Validate main fields
    main_fields_to_validate = ['temp','humidity','pressure']
    if 'main' in data:
        main = data['main']
        for field in main_fields_to_validate:
            if field not in main or not isinstance(main[field], (int, float)):
                invalid_fields.append({'field': field, 'value': main.get(field)})
            # Temperature-specific range validation
            if field == 'temp' and isinstance(main.get('temp'), (int, float)) and main['temp'] <= -100:
                invalid_fields.append({'field': 'temp', 'value': main['temp'], 'reason': 'out of valid range (> -100°C)'})
                
    # Validate wind speed field          
    if 'wind' in data:
        wind = data['wind']
        if 'speed' not in wind or not isinstance(wind['speed'], (int, float)):
            invalid_fields.append({'field': 'wind_speed', 'value': wind.get('speed')})

    if invalid_fields:
        logger.warning(f"Skipping invalid data for {city}: {invalid_fields}")
        return False

    return True