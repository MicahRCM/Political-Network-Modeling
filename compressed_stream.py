#!/usr/bin/env python3

import zstandard as zstd
import simplejson as json
import time
import pandas as pd
import csv

filename = "RC_2021-05"
# Known bots and invalid authors (e.g., [deleted], AutoModerator) that won't be useful for analysis
# bad_authors = ["[deleted]", "AutoModerator", "NFCAAOfficialRefBot", "KeepingDankMemesDank", "transcribot", "VredditDownloader", "Edgar_The_Pug_Bot", "RepostSleuthBot", "Expresstron", "transcribersofreddit", "tiny_pussy", "MTGCardFetcher", "KickItOpen_Bot", "MAGIC_EYE_BOT", "lettuce_finder", "Marketron-I", "RLCD-Bot", "nwordcountbot", "c0debrain", "SirLong3", "ChoiceEvidence", "SaveVideo", "Shakespeare-Bot"]

# Reads in CSV of authors that are invalid for analysis
bad_authors = list()
with open ("./Params/badAuthors.csv") as file_name:
    file_read = csv.reader(file_name)
    bad_authors = list(file_read)


obs = []
obs_count = 0
with open(filename + ".zst", 'rb') as fh:
    dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
    with dctx.stream_reader(fh) as reader:
        previous_line = ""
        while True:
            chunk = reader.read(2**24)  # 16mb chunks
            if not chunk:
                break

            string_data = chunk.decode('utf-8')
            lines = string_data.split("\n")
            for i, line in enumerate(lines[:-1]):
                if i == 0:
                    line = previous_line + line
                if i % 1000000 == 0:
                    print(i)
                object = json.loads(line)
                # do something with the object here
                if object['author'] not in bad_authors and object['subreddit'] == 'PoliticalCompassMemes':   
                    obs.append([object['author'], object['created_utc'], object['score'], object['subreddit'], object['author_flair_text'], object['body']])
                    if len(obs) > 5000000:
                        df = pd.DataFrame(obs)
                        df.to_csv(filename + "_a_" + str(obs_count) + ".csv", index = False)
                        obs = []
                        obs_count += 1
                        print(obs_count)
            previous_line = lines[-1]

df = pd.DataFrame(obs)
df.to_csv(filename + "_a_" + str(obs_count) + ".csv", index = False)