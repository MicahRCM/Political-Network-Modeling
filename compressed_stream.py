#!/usr/bin/env python3

import zstandard as zstd
import simplejson as json
import time
import pandas as pd

filename = "RC_2020-01-27"
# Known bots and invalid authors (e.g., [deleted], AutoModerator) that won't be useful for analysis
bad_authors = ["[deleted]", "AutoModerator", "NFCAAOfficialRefBot", "KeepingDankMemesDank", "transcribot", "VredditDownloader", "Edgar_The_Pug_Bot", "RepostSleuthBot", "Expresstron", "transcribersofreddit", "tiny_pussy", "MTGCardFetcher", "KickItOpen_Bot", "MAGIC_EYE_BOT", "lettuce_finder", "Marketron-I", "RLCD-Bot", "nwordcountbot"]

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
                object = json.loads(line)
                # do something with the object here
                if object['author'] not in bad_authors:   
                    obs.append([object['author'], object['created_utc'], object['score'], object['subreddit'], object['author_flair_text']])
                    if len(obs) > 1000000:
                        df = pd.DataFrame(obs)
                        df.to_csv(filename + "_a_" + str(obs_count) + ".csv", index = False)
                        obs = []
                        obs_count += 1
                        print(obs_count)
            previous_line = lines[-1]

df = pd.DataFrame(obs)
df.to_csv(filename + "_a_" + str(obs_count) + ".csv", index = False)