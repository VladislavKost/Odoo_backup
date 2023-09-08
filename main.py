import sys
import aiohttp
import asyncio
import base64
import configparser
import requests
import xmlrpc.client
import logging
import argparse
from tqdm.asyncio import tqdm_asyncio
from io import BytesIO
from PIL import Image


def get_parameters(config_path):
    """get_parameters(config_path)

    Get parameters from configuration file located in config_path
    config_pash: path(str)"""
    config = configparser.ConfigParser()
    config.read(config_path)
    params = {
        "url": config["Odoo"]["url"],
        "db": config["Odoo"]["db"],
        "username": config["Odoo"]["username"],
        "password": config["Odoo"]["password"],
        "planets_model": config["Odoo"]["planets_model"],
        "characters_model": config["Odoo"]["characters_model"],
        "planet_url": config["Swapi"]["planet_url"],
        "character_url": config["Swapi"]["character_url"],
        "image_url": config["Swapi"]["image_url"],
    }
    logging.basicConfig(
        level=logging.INFO,
        filename="UploadInformation.log",
        filemode="w",
        format="%(asctime)s %(levelname)s:%(message)s",
    )
    try:
        common = xmlrpc.client.ServerProxy("{}/xmlrpc/2/common".format(params["url"]))
        uid = common.authenticate(
            params["db"], params["username"], params["password"], {}
        )
        models = xmlrpc.client.ServerProxy("{}/xmlrpc/2/object".format(params["url"]))
        return params, uid, models
    except xmlrpc.client.Fault:
        logging.info(
            f'Database connection error. Incorrect database name. Current database name is "{params["db"]}"'
        )
        print("Ошибка подключения к БД. Неверное название БД.")
        sys.exit(1)
    except ConnectionRefusedError:
        logging.info(
            f'Database connection error. Incorrect localhost. Current localhost is "{params["url"]}"'
        )
        print("Ошибка подключения к БД. Неверный localhost.")
        sys.exit(1)


class Generator:
    def generate_entity_urls(self, base_url):
        """generate_entity_urls(base_url)

        Generates URL for future requests on the base of base_url
        base_url: url(str)"""
        amount = requests.get(base_url).json().get("count")
        if amount % 10 == 0:
            pages = amount // 10
        else:
            pages = amount // 10 + 1
        urls_to_request = [f"{base_url}?page={page}" for page in range(1, pages + 1)]
        return urls_to_request

    def generate_photo_urls(self, base_url, len):
        """generate_photo_urls(base_url, len)

        Generates URL for future photo requests on the base of base_url
        base_url: url(str)
        len: len of the character dict list(int)"""
        urls_to_request = [
            f"{base_url}{entity_id}.jpg" for entity_id in range(1, len + 1)
        ]
        return urls_to_request


class Asynchron:
    """Class makes asynchronous requests to the API"""

    async def get_entity(self, session, url):
        """get_entity(session, url)

        Make requests on the given URL.
        session: aiohttp.ClientSession()
        url: URL for request(str)"""
        while True:
            try:
                async with session.get(url) as response:
                    return await response.json()
            except Exception:
                logging.info(
                    f"Data acquisition error. Repeated attempt to receive data at {url}"
                )

    async def get_photo(self, session, url):
        """get_photo(session, url)

        Make requests on the given URL.
        session: aiohttp.ClientSession()
        url: URL for request(str)"""
        while True:
            try:
                async with session.get(url) as response:
                    return await response.read()
            except Exception:
                logging.info(
                    f"Data acquisition error. Repeated attempt to receive data at {url}"
                )

    async def request_all(self, urls, type="", photo=False):
        """request_all(urls, type='', photo=False)

        Make asynchronous requests on the given URL list.
        urls: list of URLs for requests(list)
        type: type of the entity: planer or character(str)
        photo: True or False (boolean)"""
        async with aiohttp.ClientSession() as session:
            if photo:
                json_list = await tqdm_asyncio.gather(
                    *[self.get_photo(session, url) for url in urls],
                    desc="Получение информации об изображениях",
                    leave=False,
                )
            else:
                json_list = await tqdm_asyncio.gather(
                    *[self.get_entity(session, url) for url in urls],
                    desc=f"Получение информации о {type}",
                    leave=False,
                )
            return json_list


class Planets:
    """Class processes json files with information about planets"""

    def generate_planet_info(self, json_list):
        """generate_planet_info(json_list)

        Processes json list file.
        json_list: list of dicts(list)"""
        planet_dict = {}
        for page in json_list:
            planets_list = page.get("results", "")
            for planet in planets_list:
                name = planet.get("name", "")
                if name == "unknown":
                    continue
                planet_url = planet.get("url")
                planet_id = int(planet_url[(planet_url[:-1].rfind("/") + 1) : -1])
                rotation_period = planet.get("rotation_period")
                orbital_period = planet.get("orbital_period")
                diameter = planet.get("diameter")
                population = planet.get("population")
                diameter, rotation_period, orbital_period, population = [
                    value if (value != "unknown" and value != "0") else ""
                    for value in [diameter, rotation_period, orbital_period, population]
                ]
                planet_dict.update(
                    {
                        planet_id: {
                            "name": name,
                            "diameter": diameter,
                            "rotation_period": rotation_period,
                            "orbital_period": orbital_period,
                            "population": population,
                        }
                    }
                )
        return planet_dict


class Characters:
    """Class processes json files with information about characters, add images and add odoo ids for planets"""

    def get_characters_info(self, json_list):
        """get_characters_info(json_list)

        Processes json list file.
        json_list: list of dicts(list)"""
        characters_dict = {}
        planet_id = ""
        for page in json_list:
            characters_list = page.get("results", "")
            for character in characters_list:
                name = character.get("name", "")
                if name == "unknown":
                    continue
                character_url = character.get("url")
                character_id = int(
                    character_url[(character_url[:-1].rfind("/") + 1) : -1]
                )
                planet_url = character.get("homeworld", "")
                if planet_url != "":
                    planet_id = int(planet_url[(planet_url[:-1].rfind("/") + 1) : -1])
                characters_dict.update(
                    {character_id: {"name": name, "planet": planet_id}}
                )
        return characters_dict

    def upgrage_characters_photo(self, characters_dict, image_dict):
        """upgrage_characters_photo(characters_dict, image_dict)

        Add image information to the characters dictionary.
        characters_dict: dict with character info(dict)
        image_dict: dict with image info(dict)"""
        changed_characters_dict = {}
        for characters_id, values in characters_dict.items():
            if image_dict.get(characters_id) != "":
                values.update({"image_1920": image_dict.get(characters_id)})
            changed_characters_dict.update({characters_id: values})
        return changed_characters_dict

    def upgrade_characters_info(self, characters_dict, ids_planets_dict):
        """upgrade_characters_info(characters_dict, ids_planets_dict)

        Add planets odoo ids information to the characters dictionary.
        characters_dict: dict with character info(dict)
        ids_planets_dict: dict with planets info(dict)"""
        new_characters_dict = {}
        for key, value in characters_dict.items():
            planet_id = value.get("planet")
            value.update({"planet": ids_planets_dict.get(planet_id, "")})
            new_characters_dict.update({key: value})
        return new_characters_dict


class CharactersImage:
    """Class generates urls for image requests and decode images"""

    def determine_response_type(self, response):
        try:
            image = Image.open(BytesIO(response))
            return "image"
        except:
            # В случае неудачи, предполагаем, что это HTML
            return "html"

    def upgrade_photo(self, image_info):
        """upgrade_photo(image_info)

        Processes decoding image
        image_info: list with image information(list)"""
        image_dict = {}
        for i in range(len(image_info)):
            character_id = i + 1
            type = self.determine_response_type(image_info[i])
            if type == "image":
                image = base64.b64encode(image_info[i]).decode("ascii")
            else:
                image = ""
                logging.info(
                    f"Image error. The character {character_id} will be uploaded without image."
                )
            image_dict.update({character_id: image})
        return image_dict


class Odoo:
    """Class checks given entity list in the current database preventing duplication and load remaining info into Odoo database"""

    def check_entity_in_odoo(self, params, base_model, uid, models, entity_dict):
        """check_entity_in_odoo(params, base_model, uid, models, entity_dict)

        Checks whether the entity is already in the Odoo.
        params: dict with parameters(dict)
        base_model: entity model in the Odoo(str)
        uid: personal id
        models: object for work with Odoo models
        entity_dict: dict with entity info(dict)"""
        try:
            result = models.execute_kw(
                params["db"],
                uid,
                params["password"],
                base_model,
                "search_read",
                [[["name", "!=", ""]]],
                {"fields": ["name"]},
            )
            new_dict = {}
            del_list = []
            ids_entity_dict = {}
            for dict in result:
                new_dict.update({dict["id"]: dict["name"]})
            entity_in_odoo_keys = list(new_dict.keys())
            entity_in_odoo_names = list(new_dict.values())
            for id_entity, entity in entity_dict.items():
                try:
                    index = entity_in_odoo_names.index(entity.get("name"))
                    del_list.append(id_entity)
                    ids_entity_dict.update({id_entity: entity_in_odoo_keys[index]})
                except ValueError:
                    continue
            for entity in del_list:
                del entity_dict[entity]
            return entity_dict, ids_entity_dict
        except xmlrpc.client.Fault:
            print(
                "Ошибка подключения к БД. Неверное имя пользователя, пароль или модель"
            )
            sys.exit(1)

    def upload_entity_info_into_oddo(
        self, params, base_model, uid, models, entity_dict, ids_entity_dict, type
    ):
        """upload_entity_info_into_oddo(params, base_model, uid, models, entity_dict, ids_entity_dict, type)

        Upload entity list into Odoo.
        params: dict with parameters(dict)
        base_model: entity model in the Odoo(str)
        uid: personal id
        models: object for work with Odoo models
        entity_dict: dict with entity info(dict)
        ids_entity_dict: dict with entity ids in  the API and in the Odoo database(dict)
        type: planet or character(str)"""
        entity_odoo_id = models.execute_kw(
            params["db"],
            uid,
            params["password"],
            base_model,
            "create",
            [list(entity_dict.values())],
        )
        ids_entity_dict.update(dict(zip(entity_dict.keys(), entity_odoo_id)))
        for id_entity, value in entity_dict.items():
            logging.info(
                f'The entity type: {type}, name: {value["name"]}, Odoo_id: {ids_entity_dict[id_entity]}, mother_id: {id_entity} is created'
            )
        return ids_entity_dict


if __name__ == "__main__":
    # Задание файла конфигураций через командную строку
    parser = argparse.ArgumentParser()
    parser.add_argument("config", help="path to the configuration file")
    args = parser.parse_args()
    config_path = args.config

    # Получение параметров программы
    params, uid, models = get_parameters(config_path)

    # Экземляр класса асинхрона
    asynchron = Asynchron()

    # Генерация url для планет
    generator = Generator()
    urls_to_request = generator.generate_entity_urls(params.get("planet_url"))

    # Получаем данные о планетах с API
    json_planets = asyncio.run(asynchron.request_all(urls_to_request, "планетах"))
    planet = Planets()
    planet_dict = planet.generate_planet_info(json_planets)

    # Загружаем планеты в Odoo
    odoo = Odoo()
    planet_dict, ids_planets_dict = odoo.check_entity_in_odoo(
        params, params["planets_model"], uid, models, planet_dict
    )
    ids_planets_dict = odoo.upload_entity_info_into_oddo(
        params,
        params["planets_model"],
        uid,
        models,
        planet_dict,
        ids_planets_dict,
        "planet",
    )
    print("Планеты загружены в Odoo")

    # Генерация url для героев
    urls_to_request = generator.generate_entity_urls(params.get("character_url"))

    # Получаем данные о героях с API
    json_characters = asyncio.run(asynchron.request_all(urls_to_request, "героях"))
    character = Characters()
    characters_dict = character.get_characters_info(json_characters)

    # Получаем фотографии героев
    get_photo = CharactersImage()
    urls_to_request = generator.generate_photo_urls(
        params.get("image_url"), len(characters_dict) + 1
    )
    photo_characters = asyncio.run(asynchron.request_all(urls_to_request, photo=True))
    image_dict = get_photo.upgrade_photo(photo_characters)

    # Добавляем фото к героям
    changed_characters_dict = character.upgrage_characters_photo(
        characters_dict, image_dict
    )

    # Загружаем героев в Odoo
    characters_dict, ids_character_dict = odoo.check_entity_in_odoo(
        params, params["characters_model"], uid, models, changed_characters_dict
    )
    new_characters_dict = character.upgrade_characters_info(
        characters_dict, ids_planets_dict
    )
    ids_character_dict = odoo.upload_entity_info_into_oddo(
        params,
        params["characters_model"],
        uid,
        models,
        new_characters_dict,
        ids_character_dict,
        "character",
    )
    print("Герои загружены в Odoo")
