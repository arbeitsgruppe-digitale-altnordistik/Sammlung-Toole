# Sammlung-Toole-Metadata in Citavi

Export metadata from Sammlung-Toole and import in Citavi. 


## Export citavified data to CSV

Either export citavified data to CSV via `metadata.py`:

1. Insert wanted handrit-URLs or -IDs in `myURLList = ["...", "...",...]`
2. and use
```python
data_c, file_name_c = get_citavified_data(myURLList)
CSVExport(file_name_c, data_c)
```

Or via the streamlit interface:

1. Toggle Search functions
2. Input handrit search URL or browse URL
3. Select Metadata or Maditadata as type of information
4. Run
5. Go to postprocessing
6. Export references to Citavi
8. Save file

## Import CSV to Citavi

### First import of Sammlung-Toole-Metadata in Citavi:

1. On the `File` / `Datei` menu in Citavi, click `Import` / `Importieren`.
2. Select `File containing tabular data` / `Aus einer Datei mit tabellarischen Daten`, click `Next` / `Weiter`.
3. Select `Plain text` / `Textdatei`, click `Next` / `Weiter`.
4. Click `Browse...` / `Durchsuchen...`, select `metadata_citavified.csv`, click `Next` / `Weiter`.
5. Check the `character encoding` / `Zeichenkodierung`, select `Unicode (UTF-8)`, click `Next` / `Weiter`.
6. Select `tab` / `Tabstopp` as `delimiter`, click `Next` / `Weiter`.
7. First row of table contains column description, select `yes` / `ja`, click `Next` / `Weiter`.
8. Table doesn't contain information, select `Not available` / `Nicht vorhanden`, click `Next` / `Weiter`.
9. Select `Archive Material` / `Archivgut`, click `Next` / `Weiter`.
10. Assign the CSV's fields to the corresponding Citavi fields: `Import definition` / `Importdefinition für Achivgut`: 
- 1/9: Handrit-ID -> `Don't import` / `Nicht importieren`
- 2/9: Creator -> `Author: Name` / `Urheber: Name`, Sequence/Reihenfolge: `Last name, first name` / `Nachname, Name`
- 3/9: Short title -> `Title` / `Kurzbetreff`
- 4/9: Description -> `Source description` / `Quellenbeschreibung`
- 5/9: Dating -> `Dating` / `Datierung`
- 6/9: Origin -> `Place of origin` / `Ursprungsort`
- 7/9: Settlement -> `Archive location` / `Ort des Archivs`
- 8/9: Archive -> `Archive` / `Archiv`
- 9/9: Signature -> `Signature` / `Signatur`
11. Select `No` / `Nein`, click `Next` / `Weiter`.
12. Save as `SammlungTooleMetadata`, click `Next` / `Weiter`.
13. Click `Add to project` / `Projekt hinzufügen`

### Further imports of Sammlung-Toole-Metadata:

1. On the `File` / `Datei` menu, click `Import` / `Importieren`.
2. Select `File containing tabular data` / `Aus einer Datei mit tabellarischen Daten`, click `Next` / `Weiter`.
3. Select `Custom [SammlungTooleMetadata]`, click `Next` / `Weiter`.
4. Click `Browse...` / `Durchsuchen...`, select `metadata_citavified.csv`, click `Next` / `Weiter`. Etc.


### In case of an error in the import definition 'SammlungTooleMetadata'

1. On the `File` / `Datei` menu, click `Import` / `Importieren`.
2. Select `File containing tabular data` / `Aus einer Datei mit tabellarischen Daten`.
3. Select checkbox `Edit` / `Bearbeiten`, click `Next` / `Weiter`. Etc.
