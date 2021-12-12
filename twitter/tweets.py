from api_auth import auth
from st_class import MyStreamListener
from tweepy import Stream
from json import loads
from pandas import DataFrame
from http.client import HTTPConnection
from urllib.parse import urlparse
from re import compile, sub


def write_csv(sitelist, name = 'temp', mode = 'w'):
    with open(name + '_sites.txt', mode) as file:
        for url in sitelist:
            file.write("%s\n" % url)


def unshorten_url(url):
    parsed = urlparse(url)
    h = HTTPConnection(parsed.netloc)
    h.request('HEAD', parsed.path)
    response = h.getresponse()
    if response.status/100 == 3 and response.getheader('Location'):
        return response.getheader('Location')
    else:
        return url


def main():
    # Create Streaming object and authenticate
    l = MyStreamListener()
    stream = Stream(auth, l)

    # This line filters Twitter Stream to capture data by keywords:
    keywords = ['education']
    stream.filter(track = keywords)

    # String of path to file: tweets_data_path
    tweets_data_path = 'education_tweets.txt'

    # Initialize empty list to store tweets: tweets_data
    tweets_data = list()

    # Read in tweets and store in list: tweets_data
    with open(tweets_data_path, "r") as tweets_file:
        for line in tweets_file:
            try:
                tweet = loads(line)
                tweets_data.append(tweet)
            except :
                continue
    # Print the keys of the first tweet dict
    print(tweets_data[0].keys())

    # Import into DataFrame
    df = DataFrame(tweets_data)

    urls = [e['urls'][0]['expanded_url'] if len(e['urls']) != 0 else None for e in df['entities']]

    write_csv(urls, name = 'education2')

    data = df[['text', 'lang']]
    data['site'] = urls

    # Look for either http://, https://, or www.
    pattern = compile('(https?://)?(www.)?')

    # Filtering out twitter sites and converting bit.ly sites to regular url site addresses
    matching = [unshorten_url(s) for s in urls if s if "twitter" not in s]

    # Transforming all http(s)://site or http(s)://www.site or www.site -> site.com
    clean = [sub(pattern, '', s) for s in matching]

    # Changing all site.com -> www.site.com
    clean_www = ['www.' + s for s in clean]

    # Write new sitelist to a text file
    write_csv(matching, name = 'clean_education2', mode='a')
    write_csv(clean, name = 'clean_education2', mode='a')
    write_csv(clean_www, name = 'clean_education2', mode='a')


if __name__ == '__main__':
    main()