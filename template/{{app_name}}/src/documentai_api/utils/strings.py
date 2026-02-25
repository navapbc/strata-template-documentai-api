"""String manipulation utilities."""


def snake_to_camel(snake_str: str) -> str:
    """Convert snake_case to camelCase."""
    components = snake_str.split("_")
    return components[0].lower() + "".join(word.capitalize() for word in components[1:])
