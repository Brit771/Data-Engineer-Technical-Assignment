from config.logging_config import logger

def validate_weather_data(city, data):
    """Validate weather data and log invalid fields."""
    invalid_fields = []

    # Check required fields
    if 'dt' not in data or not isinstance(data['dt'], int):
        invalid_fields.append({'field': 'dt', 'value': data.get('dt')})

    main_fields_to_validate = ['temp','humidity','pressure']
    if 'main' in data:
        main = data['main']
        for field in main_fields_to_validate:
            if field not in main or not isinstance(main[field], (int, float)):
                invalid_fields.append({'field': field, 'value': main.get(field)})
                
    if 'wind' in data:
        wind = data['wind']
        if 'speed' not in wind or not isinstance(wind['speed'], (int, float)):
            invalid_fields.append({'field': 'wind_speed', 'value': wind.get('speed')})

    if invalid_fields:
        logger.warning(f"Skipping invalid data for {city}: {invalid_fields}")
        return False

    return True