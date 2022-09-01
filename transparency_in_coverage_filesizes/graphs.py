import altair as alt
import polars as pl
import pandas as pd

data = {
    "source": [
        "Kaiser Permanente MRFs",
        "Project Gutenberg",
        "UnitedHealthcare MRFs",
        "Humana MRFs",
        "Anthem MRFs",
        "Cigna MRFs",
        "Aetna MRFs",
        "Libgen (text only)",
        "Libgen",
        "Library of Congress",
    ],
    "size": [
        300*20,
        700,
        8_950*20,
        47_252*12,
        16_000*20,
        0,
        380*20,
        1_000,
        120_000,  
        20_000,
    ]
}

alt.Chart(pd.DataFrame(data)).mark_bar().encode(
    y=alt.Y("source", sort = 'x', title = "Source"),
    x=alt.X("size", title = "Est. total uncompressed size (GB)"),
).save('chart.png')
