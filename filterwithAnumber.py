import pandas as pd
import re

# Dosyaları oku
cases_file = "C:/Users/Turkuaz/OneDrive/OguzLaw/Missive/cases.csv"
notes_file = "C:/Users/Turkuaz/OneDrive/OguzLaw/Missive/notes.csv"

cases_df = pd.read_csv(cases_file)
notes_df = pd.read_csv(notes_file)

cases_df = cases_df[
    cases_df['Practice Area'].str.contains(r'(?i)Defensive Asylum|BIA Appeal|Motion to Reopen|Bond Request', na=False) |
    cases_df['Case/Matter Name'].str.contains(r'(?i)Defensive', na=False)
]
"""cases_df = cases_df[
    (
        cases_df['Practice Area'].str.contains(r'(?i)Defensive Asylum|BIA Appeal|Motion to Reopen|Bond Request', na=False)
    ) &
    (cases_df['Case Closed'].astype(str).str.lower() == 'false')
]"""

# A Number formatını bulmak için regex deseni
a_number_pattern = r'\b\d{3}-?\d{3}-?\d{3}\b'

# Notes.csv'de müvekkil ismini Case Name'den ayıkla ve A Number'ı bul

def find_a_number_for_case(case_name):
    client_name = case_name.split(' - ')[0]
    # Escape client_name to treat it as a literal string in the regex
    pattern = re.escape(client_name)
    matching_notes = notes_df[
        notes_df['Case Name'].str.contains(pattern, na=False, case=False)
    ]
    
    for _, note_row in matching_notes.iterrows():
        for col in ['Subject', 'Note']:
            content = note_row.get(col, '')
            if pd.notna(content):
                match = re.search(a_number_pattern, str(content))
                if match:
                    return match.group().replace('-', '')
    return ''


# Cases.csv'ye yeni bir A Number sütunu ekle
cases_df['A Number'] = cases_df['Case/Matter Name'].apply(find_a_number_for_case)

# Son DataFrame'i oluştur
result_df = cases_df[[
    'Case/Matter Name',
    'Lead Attorney', 
    'Originating Attorney',
    'A Number'
]]

# İstenen sütunları seç
filtered_df = cases_df[['Case/Matter Name', 'Lead Attorney', 'Originating Attorney', 'A Number']]

# Sonucu kaydet
output_file = "C:/Users/Turkuaz/OneDrive/OguzLaw/Missive/defensive_cases_with_a_numbers.csv"
result_df.to_csv(output_file, index=False, encoding='utf-8')
print(f"Defensive vakalar başarıyla kaydedildi: {output_file}")
