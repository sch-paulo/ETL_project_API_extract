import requests

url = 'https://jsonplaceholder.typicode.com/comments'
headers = {
    'Accept': 'application/json',
    'User-Agent': 'MinhaAplicacao/1.0'
}
params = {'currency': 'USD'}

response = requests.get(url, headers=headers, params=params)
data = response.json()
print('Preço do Bitcoin (USD):', data["data"]["amount"])