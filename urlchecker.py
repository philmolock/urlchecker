import csv, os, sys, threading, datetime, requests, time

# Settings
customerName = 'JCP'
# Directory | Location of CSV exports from BAM and UCM-A

directorySettings = {'accounts':'accounts','campaigns':'campaigns','ad groups':'adgroups','keywords':'keywords', 'output':'output', 'ads':'ads'}

# Keyword Normalization | Prepositions to ignore, brand keywords to normalize

# Ignore List | Accounts, Campaigns, or Ad Groups to ignore
# ignoreList = {'accounts':[],'campaigns':[],'ad groups':[],'keywords':[]}
accountList = ['X0620968','F121QYDH','F121RGKQ','F121Q5FQ','F1211P4N','F121P4CS','F1212VSS','F121GS3P','F12197C8','F121QWD3','F121P39G','F121YJ4L','F121NVVE','F121TG42']
# Header Settings | Choose the order in which your columns appear
outputHeader = ['Eligibility','Type','Status','Id','Account Number', 'Campaign','Ad Group','Final Url']
outputHeaderBroken = ['Eligibility','Type','Status','Id','Account Number', 'Campaign','Ad Group','Final Url', 'Status Code']
checkedURLs = {}


# Helper Functions | Functions to assist

# Generate random string for unique file names using datetime
def getDateTimeNow():
    dateTimeNow = datetime.datetime.now()
    return f'{dateTimeNow.strftime("%y")}{dateTimeNow.strftime("%d")}{dateTimeNow.strftime("%y")}{dateTimeNow.strftime("%H")}{dateTimeNow.strftime("%M")}{dateTimeNow.strftime("%f")}'

# Core | Core functions

# Get list of CSV's from directory as specified in settings
def getAccountCSVs():
    try:
        return [item for item in os.listdir(directorySettings['accounts']) if '.csv' in item]
    except:
        print(f"Error accessing the {directorySettings['accounts']} directory, please make sure you've created this folder and stored your Account CSV's there.")
        return False

def getCampaignCSVs():
    try:
        return [item for item in os.listdir(directorySettings['campaigns']) if '.csv' in item]
    except:
        print(f"Error accessing the {directorySettings['campaigns']} folder, please make sure you've created this folder and stored your Campaign CSV's there.")
        return False

def getAdGroupCSVs():
    try:
        return [item for item in os.listdir(directorySettings['ad groups']) if '.csv' in item]
    except:
        print(f"Error accessing the {directorySettings['ad groups']} folder, please make sure you've created this folder and stored your Ad Group CSV's there.")
        return False

def getAdCSVs():
    try:
        return [item for item in os.listdir(directorySettings['ads']) if '.csv' in item]
    except:
        print(f"Error accessing the {directorySettings['ads']} folder, please make sure you've created this folder and stored your Ads CSV\'s there.")
        return False

def createOutputDir():
    try:
        os.mkdir(directorySettings['output'])
    except FileExistsError:
        print(f"Folder {directorySettings['output']} already exists.")
      
# Advance a BAM csv to the first line of data and return its header
def advanceBAMCSVtoData(readerCSV):
    while True:
        currentRow = next(readerCSV)
        if 'Type' in currentRow[0]:
            header = currentRow
        elif 'Format Version' in currentRow[0]:
            break
    return header

# Take a header row and column and return the index of the list
def getIndexOfHeader(header, column):
    if column in header:
        return header.index(column)

def directoryChecks():
    createOutputDir()
    getAccountCSVs()
    getCampaignCSVs()
    getAdGroupCSVs()
    getAccountCSVs()

# Returns True if a keyword is eligible to serve, False if ineligible to serve
def checkKeywordEligibility(row, header, accounts, adGroups, campaigns):

    if row[getIndexOfHeader(header,'Status')] != 'Active':
        return False, 'Keyword Inactive'
    # if ad group is paused return false
    try:
        if adGroups[row[getIndexOfHeader(header,'Parent Id')]]['Status'] != 'Active':
            return False, 'Ad Group Inactive'
    except KeyError:
        return False, 'Ad Group Oprhan'

    # if campaign is paused return false
    try:
        if campaigns[adGroups[row[getIndexOfHeader(header,'Parent Id')]]['Parent Id']]['Status'] != 'Active':
            return False, 'Campaign Inactive'
    except KeyError:
        return False, 'Campaign Orphan'
    # if account is paused return false
    try:
        if accounts[campaigns[adGroups[row[getIndexOfHeader(header,'Parent Id')]]['Parent Id']]['Parent Id']]['Status'] != 'Active':
            return False, 'Account Inactive'
    except KeyError:
        return False, 'Account Orphan'

    return True

def checkAdEligibility(row, header, accounts, adGroups, campaigns):

    if row[getIndexOfHeader(header,'Status')] != 'Active':
        return False, 'Ad Inactive'
    
    # if campaign is paused return false
    try:
        if campaigns[adGroups[row[getIndexOfHeader(header,'Parent Id')]]['Parent Id']]['Status'] != 'Active':
            return False, 'Campaign Inactive'
    except KeyError:
        return False, 'Campaign Orphan'
    # if account is paused return false
    try:
        if accounts[campaigns[adGroups[row[getIndexOfHeader(header,'Parent Id')]]['Parent Id']]['Parent Id']]['Status'] != 'Active':
            return False, 'Account Inactive'
    except KeyError:
        return False, 'Account Orphan'

    return True

def loadAccounts():
    accounts = {}
    accountsCSVs = getAccountCSVs()
    if accountsCSVs:
        for accountCSV in accountsCSVs:
            with open(f"{os.getcwd()}\\{directorySettings['accounts']}\\{accountCSV}") as openAccountCSV:
                accountReader = csv.reader(openAccountCSV)
                header = next(accountReader)
                for row in accountReader:
                    accounts[row[getIndexOfHeader(header,'Account No.')]] = {'Status':row[getIndexOfHeader(header,'Account Status')]}
    return accounts

def loadAdGroups():
    adGroups = {}
    adGroupCSVs = getAdGroupCSVs()
    if adGroupCSVs:
        for adGroupCSV in getAdGroupCSVs():
            with open(f"{os.getcwd()}\\{directorySettings['ad groups']}\\{adGroupCSV}") as openAdGroupCSV:
                adGroupReader = csv.reader(openAdGroupCSV)
                header = advanceBAMCSVtoData(adGroupReader)
                for row in adGroupReader:
                    adGroups[row[getIndexOfHeader(header,'Id')]] = {'Status': row[getIndexOfHeader(header,'Status')], 'Parent Id': row[getIndexOfHeader(header,'Parent Id')]}
    return adGroups

def loadCampaigns():
    campaigns = {}
    campaignCSVs = getCampaignCSVs()
    if campaignCSVs:
        for campaignCSV in campaignCSVs:
            with open(f"{os.getcwd()}\\{directorySettings['campaigns']}\\{campaignCSV}") as openCampaignCSV:
                campaignReader = csv.reader(openCampaignCSV)
                header = advanceBAMCSVtoData(campaignReader)
                for row in campaignReader:
                    campaigns[row[getIndexOfHeader(header,'Id')]] = {'Status': row[getIndexOfHeader(header,'Status')], 'Parent Id': row[getIndexOfHeader(header,'Account Number')]}
    return campaigns

def getStatusCode(url, header):
    if url in checkedURLs:
        if not checkedURLs[url] >= 200 or not checkedURLs[url] <= 299:
            return (False, checkedURLs[url])
        else:
            return (True, checkedURLs[url])
    else:
        time.sleep(1)        
        r = requests.get(url=url, headers=header, allow_redirects=False)
        checkedURLs[url] = r.status_code
        print(f'Checking {url} \t received status code {r.status_code}')
        if not r.status_code >= 200 or not r.status_code <= 299:
            return (False, r.status_code)
        return (True, r.status_code)

def checkAdFinalURLS():
    accounts = loadAccounts()
    adGroups = loadAdGroups()
    campaigns = loadCampaigns()
    adCSVs = getAdCSVs()
    httpHeader = {'User-agent' : 'Mozilla/5.0'}

    with open(f"{os.getcwd()}\\{directorySettings['output']}\\{customerName} Missing Final URLs by Active Ads {getDateTimeNow()}.csv", 'w', newline='') as missingUrlsActive:
        with open(f"{os.getcwd()}\\{directorySettings['output']}\\{customerName} Missing Final URLs by Inactive Ads {getDateTimeNow()}.csv", 'w', newline='') as missingUrlsInactive:
            with open(f"{os.getcwd()}\\{directorySettings['output']}\\{customerName} Broken URLs by Active Ads {getDateTimeNow()}.csv", 'w', newline='') as brokenUrlsActive:
                writerMissingUrlsActive = csv.writer(missingUrlsActive)
                writerMissingUrlsActive.writerow(outputHeader)
                writerMissingUrlsInactive = csv.writer(missingUrlsInactive)
                writerMissingUrlsInactive.writerow(outputHeader)
                writerBrokenUrlsActive = csv.writer(brokenUrlsActive)
                writerBrokenUrlsActive.writerow(outputHeaderBroken)

                for adCSV in adCSVs:
                    print(f"Reading Ad CSV [{adCSVs.index(adCSV) + 1}/{len(adCSVs)}]...\t {adCSV}")
                    with open(f"{os.getcwd()}\\{directorySettings['ads']}\\{adCSV}", errors='ignore') as openedAdCSV:
                        readerAdCSV = csv.reader(openedAdCSV)
                        header = advanceBAMCSVtoData(readerAdCSV)
                        header[0] = 'Type'

                        for row in readerAdCSV:
                            if row[getIndexOfHeader(header, 'Account Number')] not in accountList:
                                continue
                            else:
                                eligibility = checkAdEligibility(row, header, accounts, adGroups, campaigns)
                                if not row[getIndexOfHeader(header, 'Final Url')] and not eligibility[0]:
                                    newRow = [eligibility[1]]
                                    for item in outputHeader[1:]:
                                        newRow.append(row[getIndexOfHeader(header, item)])
                                    writerMissingUrlsInactive.writerow(newRow)
                                elif not row[getIndexOfHeader(header, 'Final Url')] and eligibility:
                                    newRow = ['Eligible']
                                    for item in outputHeader[1:]:
                                        newRow.append(row[getIndexOfHeader(header, item)])
                                    writerMissingUrlsActive.writerow(newRow)
                                elif row[getIndexOfHeader(header, 'Final Url')] and eligibility:
                                    statusCheck = getStatusCode(row[getIndexOfHeader(header, 'Final Url')], httpHeader)
                                    if statusCheck[0]:
                                        continue
                                    else:
                                        newRow = ['Eligible']
                                        for item in outputHeaderBroken[1:len(outputHeaderBroken)-1]:
                                            newRow.append(row[getIndexOfHeader(header, item)])
                                        newRow.append(statusCheck[1])
                                        writerBrokenUrlsActive.writerow(newRow)
                                        print(f"We caught one: {row[getIndexOfHeader(header, 'Final Url')]}\t{statusCheck[1]}")
                                        
                                


# Main | Executes script

def main():

    directoryChecks()
    checkAdFinalURLS()

main()

# To pull out of keyword row:
