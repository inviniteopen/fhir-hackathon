Arkkitehtuuri:

- integraatiokerros: luetaan data JSONista duckdb dataframeiksi. taulu per resurssityypi. tallenetaan persistent tietokantaan.

- skeematestikerros: testataan taulun arvojen skeeman mukaisuus

- agregaatiotestikerros: korkeamman tason testejä, esim. testidata noudattaa tiettyä jakaumaa

- exploraatio / visualisaatio kerros -> kuvaajia datasta. esim jokin marimo notebook


Datan käsittely:

- luetaan FHIR data pronssi kerrokseen -> enimäkseen nested JSON objekteja

- flatataan data silver tauluun -> ei kaikkia kenttiä, jotkin valitut taulut + valitut columnit

- ajetaan testit valituille kolumneille -> esim. enum oikeanlainen, numero halutussa rangessa

- tallennetaan silver tason tauluun uuteer error columniin lista erroreista joita validaatiossa noussut