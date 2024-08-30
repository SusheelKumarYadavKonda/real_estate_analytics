import http.client

conn = http.client.HTTPSConnection("zillow-com1.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "4ac1b0e68dmshc8392288871d1b0p109887jsnb79f0c56fee4",
    'x-rapidapi-host': "zillow-com1.p.rapidapi.com"
}

conn.request("GET", "/rentEstimate?address=1093%20County%20Route%2060%2C%20Newton%20Falls&d=0.5&propertyType=SingleFamily", headers=headers)

res = conn.getresponse()
data = res.read()

print(data.decode("utf-8"))