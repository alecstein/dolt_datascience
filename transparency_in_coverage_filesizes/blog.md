<<<<<<< HEAD
---
title: "A trillion prices"
date: "2022-09-02"
author: "Alec Stein"
authorHref: "https://www.dolthub.com/team#alec"
tags: "bounty"
---

## American insurers just published a trillion hospital prices
=======
American insurers just published a trillion hospital prices
>>>>>>> 064c533d81b1a93ba94fdfae71fccd0788ee679a

The files needed to reproduce the numbers in this blog are here:
https://github.com/alecstein/transparency-in-coverage-filesizes

On July 1, 2022, insurance companies dumped hundreds of terabytes of data into the public domain. That is, files that contain every price negotiated with every healthcare provider for every procedure they offer. 

<<<<<<< HEAD
This adds up to around a trillion separate prices. As for how it's possible to slice and dice prices up on that scale, I don't have a clue. But you don't have to take my word for it: you can see it for yourself.
=======
This adds up to around a trillion separate prices. As for how it's possible to slice and dice prices up on that scale, I don't have a clue. But you can see for yourself.
>>>>>>> 064c533d81b1a93ba94fdfae71fccd0788ee679a

Humana released [nearly 500,000 compressed CSV files](https://developers.humana.com/Resource/PCTFilesList?fileType=innetwork) totaling 50TB compressed (~600TB uncompressed.) At around 70k prices per 9MB file, this translates into about 400 billion individual prices negotiated with different providers.

This appears to be one of the largest file dumps, but other payers are similar in their largesse. On [United Healthcare's page](https://transparency-in-coverage.uhc.com/) they list over 55,000 individual files for download. Together these make up 9TB of compressed JSON, or around 250TB uncompressed. A back-of-the-envelope estimate suggests they've published around a 100 billion negotiated prices.

<<<<<<< HEAD
Aetna and Anthem have also published enormous amounts of data.

Insurers [did not want to publish this data](https://www.healthcaredive.com/news/payers-employers-argue-price-transparency-push-wont-help-consumers/571393/) but they effectively lost: the data is out there. Some have made it harder to get than others. Cigna, for example, makes you copy and paste a large link, which takes you to a page with files that apparently cannot be downloaded ([try it yourself](https://www.cigna.com/legal/compliance/machine-readable-files)). Anthem's site [hosts simply a broken link](https://www.anthem.com/machine-readable-file/search/).

![cigna](../images/sl-cigna.png)

But barring a few exceptions, the insurance companies that have complied have, for the most part, completely complied. And that's kind of the problem.

There's just *so much data.*

## How much is a lot?

And that's just how much data gets uploaded *per month*: probably 100TB compressed. Easily petabytes uncompressed. Just to store this data costs tens of thousands of dollars.

To download that much data per month, you'll need to be sure to have a business-level fiber-optic connection that can handle 400Mbps.

## The value in the data

The value in this data is being able to query it, reshape it, and analyze it. Maybe your employer getting a shitty deal, but how can you know unless you can compare plans?

And right now, with mountains of terabytes of JSON and CSV files out there, that seems almost impossible to imagine for all but the most dedicated teams.

```
Insurers (all compressed)
Empire BCBS (serving 28 New York counties): .05TB
Aetna: .3TB
Kaiser: 2.2TB
United: 9TB
Humana: 50TB
Anthem: ??? Broken
Cigna: ???
```
=======
Insurers [lobbied against the change](https://www.healthcaredive.com/news/payers-employers-argue-price-transparency-push-wont-help-consumers/571393/) and lost: the data is out there. But the value in the data is being able to query it. And right now, with mountains of terabytes of JSON and CSV files out there, that seems almost impossible to imagine for all but the most dedicated teams.

Insurers (all compressed)
United: 9TB
Humana: 50TB
Aetna: .3TB
Anthem: Working on it
Cigna: ???
>>>>>>> 064c533d81b1a93ba94fdfae71fccd0788ee679a

## How is this even possible

I naively thought that insurers would be publishing the prices they negotiated with the ten thousand-or-so hospitals and clinics the US. I was wrong.

There are over 1M physicians and even more "provider groups" which are clusters of physicians/corporations that take the same price for a given procedure. Insurance companies have published negotiated rates for all of these.

With around 20,000 different procedures, and (say) 500,000 different provider groups -- plus about ten different "variations" on each price (say, for certain code suffixes or service codes) -- you can handwave an estimate of about 100,000,000,000 different prices per insurer.

## Maybe we can build this database together

Having this database be open and easy to query is one of the best shots we have at peeking behind the curtain that conceals the insurance marketplace. Though it's not easy to do, we've taken some steps towards making it possible.

1. We wrote a python script to flatten JSONL files, which are streamable
1. 90GB JSON files can't be streamed, so we built a JSON -> JSONL converter in Go to stream the files
1. Humana has already done the hard work of giving us the flattened schema we need to import the data into a relational DB
1. We can limit ourselves to the 70 CMS-required shoppable services (services that you don't need urgently)


########## EVERYTHING BELOW HERE IS OLD AND NOT YET PART OF THE BLOG ##################

Extraneous info:

In 2018 Donald Trump signed into law a bill that was supposed to shake up the American healthcare system. The bill, the [Hospital Price Transparency and Disclosure Act of 2018](https://www.congress.gov/bill/115th-congress/house-bill/6508/text), required hospitals to publish their prices in a legible way for consumers and, specially, in a machine-readable format for researchers. 

The Center for Medicaid Services (CMS) came up with the requirements and have the power of oversight.

The transparency bill was supposed to make price comparison easier than ever. But in reality, weak schema requirements and negligible penalties for noncompliance resulted in a lot of garbage data, and some hospitals just flouting the law entirely.

A new approach which went into effect July 1st promised to learn from the CMS's past failures. This time the CMS targeted insurance companies directly, going after their pristine data, instead of 6000 or so individual hospitals. They completely specified the schema, created a schema validation tool, even going so far as to specify a specific format for the filenames.

Before July 1st, it really seemed to me that this would be the last word on hospital price transparency. But the catch only appeared when I went to download the data.

I looked at United Healthcare's Transparency in Coverage page. The first shock comes when the 25MB page loads, revealing over 55,000 perfectly-formatted links. Indeed, they contain all of the required data in the appropriate format. Over the wire, these links translate to about 9TB of compressed JSON. To download that, it would max out a typical home connection for an entire month. 

And that's just to download it. To store it, the inflated JSON, you'd need between 200 and 300TB of space. [We need to know what this would be in a mongodb]. Of course, we want to be able to do more than just hold the data, but query it efficiently.

We experimented with different flattening approaches to see how much we could reduce storage and time some queries, but the file sizes quickly outgrew what's currently possible to put in a Dolt database.

The size of these files raises another question: what's in them? How are they so big?

After we flattened the JSON, we learned that, 