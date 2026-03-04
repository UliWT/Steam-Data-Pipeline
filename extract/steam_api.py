import requests

BASE_URL = "https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/"

def get_player_count(app_id):

    url = f"{BASE_URL}?appid={app_id}"

    response = requests.get(url, timeout=10)

    data = response.json()

    player_count = data["response"].get("player_count")

    return player_count

def get_game_metadata(app_id):

    url = f"https://store.steampowered.com/api/appdetails?appids={app_id}"

    response = requests.get(url, timeout=10)

    data = response.json()

    if data[str(app_id)]["success"]:

        game_data = data[str(app_id)]["data"]

        name = game_data.get("name")
        developers = game_data.get("developers")
        publishers = game_data.get("publishers")

        return{
            "app_id": app_id,
            "name": name,
            "developer": developers[0] if developers else None,
            "publisher": publishers[0] if publishers else None 
        }
    
    return None