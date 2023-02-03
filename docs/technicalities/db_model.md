# Database Model

The database model still needs some streamlining. 
There are open GitHub issues for that.

The model currently contains the following entities:

- `Manuscript`:  
  The physical, text-bearing object. 
  Can be a full manuscript or a fragment.
- `Catalogue Entry`:  
  A description of a `Manuscript` in a catalogue. 
  There can be multiple `Catalogue Entries` describing a single `Manuscript`, 
  which may just be in different languages, 
  vary in their extent,
  or even have contradicting information.
- `Person`:  
  A person that is connected in one or another way to a `Manuscript` or `Catalogue Entry`.
- `Text`:  
  A text, as contained by a manuscript.
- `Group`:  
  A user defined grouping of `Manuscripts`, `People` or `Texts`.  
  Groups are used to save and combine search results, 
  in order to display the results together.

## Entity Relationship Diagram

```mermaid
erDiagram
    Manuscript {
        string manuscript_id PK
        string shelfmark
        integer catalogue_entries
        string catalogue_ids
        string catalogue_filenames
        string title
        string description
        string date_string
        integer terminus_post_quem
        string termini_post_quos
        integer terminus_ante-quem
        string termini_ante_quos
        integer date_mean
        float date_standard_deviation
        string support
        integer folio
        string height
        string width
        string extent
        string origin
        string creator
        string country
        string settlement
        string repository
    }
    CatalogueEntry {
        string catalogue_id PK
        string shelfmark
        string manuscript_id
        string catalogue_filename
        string title
        string description
        string date_string
        integer terminus_post_quem
        integer terminus_ante-quem
        integer date_mean
        integer dating_range
        string support
        integer folio
        string height
        string width
        string extent
        string origin
        string creator
        string country
        string settlement
        string repository
    }
    Person {
        string pers_id PK
        string first_name
        string last_name
    }
    Text {
        string text_id PK
    }
    Group {
        UUID group_id
        enum group_type "ManuscriptGroup, TextGroup, PersonGroup"
        string name
        sting date
        string items
    }
    CatalogueEntry }|--o{ Person : "mentions"
    Manuscript }|--o{ Person : "is related to"
    CatalogueEntry }|--o{ Text : "mentions"
    Manuscript }|--o{ Text : "is related to"
```

Where string values represent a list of values, those values are concatenated with `|`. 
In the future, these relationships will be modelled as one-to-many relationships in the database.
