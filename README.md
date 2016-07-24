# Cooper Hewitt
For many years, approximately four years, the Cooper Hewitt design museum was closed while it
underwent a transformation.  During this time they overhauled their technology to create
an amazing experience for the visitor.  They wanted it to take the form of an engaging immersive
experience, whereby visitors would interact with the technology.  During this time "The Pen" was
born which allowed the museum to track which artworks (objects) were being interacted with via
recorded timestamps and identifiers.  Recently this information has been made openly available to the public,
in de-identified bundles to protect the privacy of the visitors.


## Project Description
Aggregating the Pen data and collection metadata (metadata about an object) can give further insights
into how the museum is effectively using their assets and resources, notably "The Pen" and how exhibitions
are planned and artworks chosen.  As the museum is heavily focused on improving the experience factor for
the visitors, understanding the behavioral patterns and tastes would be a value-add to the them.


## Cooper Hewitt Data Sources
- [De-identified Pen Data](https://github.com/cooperhewitt/the-pen-data/)
    - The museum provides a digital pen to each visitor upon entry to tag artwork they are interested in.  
    In addition they can use the pen to draw shapes that are most similar to these artworks at select stations,
    which will bring up the associated artwork metadata at the station.  Each time the pen tags an object (artwork);
    it is recorded with a timestamp.  All this data is de-identified data recorded as “bundles”.

- [Collection Metadata](https://collection.cooperhewitt.org/api/methods/)
    - Object artwork metadata has been exposed through restful API’s via multiple endpoints.  Curators have
    digitized object metadata where available; therefore not all metadata may be available for a particular artwork,
    nor normalized in any format.  This metadata will provide additional features and context to the object being
    tagged.  Included in this metadata, are the locations (rooms, spots) where artwork has appeared.


## Configuration (API Keys)
- Fill in the appropriate sections in config/api_cred.yml based on registration with Cooper Hewitt.

## Data (Transformed)
- collection_objects {.csv, .tsv, .pkl}
    - Represents some of the metadata about a given artwork
- exhibitions {.csv, .pkl}
    - Represents when an exhibition took place
- exhibition_objects {.json, .pkl}
    - Represents which objects are associated with an exhibition (not directly associated w/pen data timestamps)
- pen_collected_items {.csv, original}
    - De-identified Tracked Pen data for a particular visitor

## Metrics
- [TBD]

## Cooper Hewitt Resources
- [Cooper Hewitt Labs](http://labs.cooperhewitt.org)
- [Collection Stats](http://collection.cooperhewitt.org/stats)
- Articles
    - [Citizen Curation](http://tfmorris.blogspot.com/2012/10/citizen-curation-of-smithsonian-metadata.html)
    - [Happy Birthday for the Pen](http://labs.cooperhewitt.org/2016/a-very-happy-open-birthday-for-the-pen/)
