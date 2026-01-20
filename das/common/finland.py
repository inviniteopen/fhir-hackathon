from datetime import date

import holidays


def get_finnish_holidays(end_year: int, start_year: int = 2000) -> list[date]:
    """Returns a list of datetime.date objects representing Finnish holidays."""
    assert start_year <= end_year, (
        "The input parameter end_year is larger than the start_year."
    )

    year_list = list(range(start_year, end_year + 1))
    return list(holidays.Finland(years=year_list).keys())
