Arkkitehtuuri:

- integraatiokerros: luetaan data JSONista duckdb dataframeiksi. taulu per resurssityypi. tallenetaan persistent tietokantaan.

- skeematestikerros: testataan taulun arvojen skeeman mukaisuus

- agregaatiotestikerros: korkeamman tason testejä, esim. testidata noudattaa tiettyä jakaumaa

- exploraatio / visualisaatio kerros -> kuvaajia datasta. esim jokin marimo notebook