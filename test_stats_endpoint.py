import dotenv
import os
import requests

dotenv.load_dotenv('.env.test')

def get_stats_data(url):
    service_token = os.environ.get('GRAFANA_SERVICE_TOKEN')
    headers = {'Authorization': f'Bearer {service_token}'}
    r = requests.get(url, headers=headers)
    return [r.json(), r.status_code]


def get_earnings(platform, hours):
    end = round(time.time())
    domain = (
            os.environ.get("STATS_URL") +
            f"api/datasources/uid/dec5owmc8gt8ge/resources/api/v1/query?query="
            f'sum(increase(payment_amount_received%7Bexported_job%3D~"{settings.GRAFANA_JOB_NAME}"%2C%20platform%3D"{platform}"%7D%5B{hours}%5D)%2F10%5E9)&time={end}'
    )
    data = get_stats_data(domain)
    if data[1] == 200 and data[0]["data"]["result"]:
        return round(float(data[0]["data"]["result"][0]["value"][1]), 2)
    return 0.0

def test_earnings_endpoint():
    platforms = ["erc20-goerli-tglm", "erc20-holesky-tglm", "erc20-mumbai-tglm"]
    hours = 24
    for platform in platforms:
        earnings = get_earnings(platform, hours)
        assert isinstance(earnings, float)
        assert earnings >= 0.0