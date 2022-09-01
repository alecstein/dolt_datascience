# How much data did the insurance industry just dump?

This repository was my attempt to figure that out.

### Cigna

No data available. Files are corrupted.

https://www.cigna.com/legal/compliance/machine-readable-files

### Anthem

Files downloaded by going to their main page:

https://www.anthem.com/machine-readable-file/search/

and downloading their index. I then split this using [jsplit](https://github.com/dolthub/jsplit), our in-house tool made by Brian Heni, to makeit into JSONL format. Then I streamed the lines and counted the URLs and their sizes.

### Humana

https://developers.humana.com/Resource/PCTFilesList?fileType=innetwork

Scraper included.

### UnitedHealthcare

https://transparency-in-coverage.uhc.com/

Scraper included.

### Aetna

https://health1.aetna.com/app/public/#/one/insurerCode=AETNACVS_I&brandCode=ALICSI/machine-readable-transparency-in-coverage?searchTerm=97109000&lock=true

Scraper included.

### EmpireBC

https://www.empireblue.com/machine-readable-file/search/

Scraper included.

### BCBS North Carolina

https://pstage.bluecrossnc.com/about-us/policies-and-best-practices/transparency-coverage-mrf

Scraper included, but the largest JSON file is malformed.