# ---
# title: "Pull users from elastic results"
# author: "Alejandro Robles, Jr. Analyst"
# date: "September 25, 2017"
# purpose: "Once you download the verified sites from polaris, automatically drop the site list
#   and tdid uses (in json format) to dropbox
# ---


from pandas import read_table
from numpy import unique, savetxt
from os.path import expanduser
from json import dump
from pyodbc import connect
from sys import exit
from time import time

# ========= This is your input =========
v_username = 'alejandro.robles'
v_password = 'I love code7'


# ===== The rest don't worry about ====

home = expanduser("~")
dropboxpath = home + "\Dropbox (The Trade Desk)\\Data Analytics Classifier Files\\Archive\\1_SITE LIKE\\"
downloadspath = home + "\\Downloads\\"
documentspath = home + "\\Documents\\"
sitespath = dropboxpath + "keyword_input\\september\\site lists\\Python - Sitelist\\"
sqlpath = dropboxpath + "keyword_input\\september\\queries\\Python\\"
jsonpath = dropboxpath +  "keyword_input\\september\\json_txtfiles\\Python_TDID\\"

query = """
select count(DISTINCT tdid) as uniques
from ttd.bidfeedback bf  
where logentrytime >= getutcdate()- {}
and site in ({})
"""

query2 = """
select DISTINCT tdid as uniques
from ttd.bidfeedback bf  
where logentrytime >= getutcdate()- {}
and site in ({})
"""


def importsites(filename, path = downloadspath):
    fullpath = path + filename
    print "Importing sitelist from :" + fullpath
    df = read_table(fullpath)
    if df.shape[1] == 3:
        df.columns = ['sites', 'label', 'good_sites']
        uniques = unique(df[df.good_sites == True].sites).tolist()
    elif df.shape[1] == 1:
        df.columns = ['sites']
        uniques = unique(df.sites).tolist()
    else:
        print 'The format of your text file is unfamiliar so will exit program... sorry'
        exit()
    print "There are " + str(len(uniques)) + " unique sites."
    return {"string" : str(uniques)[1:-1], "list" : uniques }

def site2txt(filename, sitelist, path = sitespath):
    print 'Exporting sitelist to ' + path + filename
    savetxt(path + filename, sitelist, fmt= '%5s')

def count_users(sites, lookbackwindow = 90):
    print "Counting the users..."
    with connect(driver='{Vertica}', server='tor.vertica.adsrvr.org', database='theTradeDesk', port=5433,
                        uid=v_username, pwd=v_password) as conn:
        cursor = conn.cursor()
        count = cursor.execute(query.format(lookbackwindow, sites)).fetchone()
        if count:
            print "There are " + str(count.uniques) + " users."
            if count.uniques < 1000000:
                continuepull = raw_input('There is less than 1 million users, do still want the pull the users? (y/n): ')
                if continuepull == 'n':
                    print 'You said no, so we are going to terminate the program ... bye!'
                    exit()

def pull_users(filename, sites, tpd = "Placeholder", lookbackwindow = 90):
    print "Pulling the users..."
    fullpath = filename
    with connect(driver='{Vertica}', server='tor.vertica.adsrvr.org', database='theTradeDesk', port=5433,
                        uid=v_username, pwd=v_password) as conn:
        cursor = conn.cursor()
        cursor.execute(query2.format(lookbackwindow, sites))
        tdids = cursor.fetchall()
        print "Got the users, now dumping them to a text file and exporting to ..." + fullpath
        with open(fullpath, 'w') as outfile:
            for i, tdid in enumerate(tdids):
                j = {"TDID": tdid[0], "Data": [{"Name": tpd, "TtlInMinutes": 129600}]}
                dump(j, outfile)
                outfile.write('\n')
                if i < 10:
                    print str(j)

def main():
    start_time = time()

    path = 'C:\\Users\\Alejandro.Robles\\PycharmProjects\\Twitter\\'

    nameoftxtfile = raw_input('Please enter the name of the text file containing the sitelist (downloaded from polaris):\n')
    lookbackwindow = input('What do you want your lookbackwindow to be? (Default is 90): ')
    tpd = raw_input('Please input thirdpartydataelement for the json to input? (If none, input "Placeholder"): ' )
    sites = importsites(nameoftxtfile, path)
    site2txt(nameoftxtfile, sites['list'], path + 'sitelist\\')
    count_users(sites['string'], lookbackwindow)
    pull_users('users' + nameoftxtfile , sites["string"], tpd, lookbackwindow)

    print " Total time is %d minutes %d seconds" % (
        round(time() - start_time, 1) // 60, round(time() - start_time, 1) % 60)
    print "Remember to load the files into S3!"



if __name__ == "__main__":
    main()

