import urllib3
from bs4 import BeautifulSoup
import time

req = urllib3.PoolManager()

# gets eksi titles
def get_eksi():
    res = req.request('GET', "https://eksisozluk1923.com/")
    soup = BeautifulSoup(res.data, 'html.parser')
    titles = soup.select("ul li a[href$='popular']")

    data = []
    date = time.strftime("%d.%m.%Y", time.localtime())
    hour = time.strftime("%H:%M", time.localtime())

    for title in titles:
        data.append(' '.join(title.getText().split()[:-1]) + ',' + date + ',' + hour + '\n')

    return data

def main():
    data = get_eksi()

    f = open('./eksi.csv', 'a', encoding='UTF-8')
    f.writelines(data)

if __name__ == '__main__':
    main()