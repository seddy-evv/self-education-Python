# The function takes as input a list of countries and cities for each country.
# For each city from the list with requests, the function prints the name of
# the country to which the city belongs.
# Initial data:
# n - number of countries
# countries - string with countries
# m - number of requests
# cities - string with cities


def find_country(n, countries_str, m, cities_str):
    list_of_countries = countries_str.split("\n")
    list_of_cities = cities_str.split("\n")
    country_city = {}
    for i in range(n):
        country, *cities = list_of_countries[i].split()
        dict_ = dict().fromkeys(cities, country)
        for city in dict_:
            country_city[city] = dict_[city] + " " + country_city.get(city, "")

    for i in range(m):
        print(country_city.get(list_of_cities[i].strip()))


string_of_countries = """Russia Moscow Petersburg Novgorod Kaluga
                         Ukraine Donetsk Odessa
                         Belarus Minsk Gomel Brest
                         France Paris Brest"""
string_of_cities = """Odessa
                      Moscow
                      Novgorod
                      Gomel
                      Brest"""


if __name__ == '__main__':
    find_country(4, string_of_countries, 5, string_of_cities)
