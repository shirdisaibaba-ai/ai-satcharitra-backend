#!/usr/bin/env python3
"""
Script to convert PostgreSQL backup data to MongoDB format for Sai Satcharitra chapters
"""

import re
from datetime import datetime
from typing import Dict, List, Any

def parse_copy_data(file_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Parse the PostgreSQL COPY data from backup file"""
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1', 'iso-8859-1']
    content = None
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                content = f.read()
            break
        except UnicodeDecodeError:
            continue
    
    if content is None:
        raise ValueError("Could not decode file with any encoding")
    
    # Extract chapters data
    chapters_match = re.search(
        r'COPY public\.chapters.*?FROM stdin;\n(.*?)\n\\\.', 
        content, 
        re.DOTALL
    )
    
    # Extract chapter_translations data  
    translations_match = re.search(
        r'COPY public\.chapter_translations.*?FROM stdin;\n(.*?)\n\\\.', 
        content, 
        re.DOTALL
    )
    
    if not chapters_match or not translations_match:
        raise ValueError("Could not find chapters or translations data")
    
    # Parse chapters
    chapters_data = []
    for line in chapters_match.group(1).strip().split('\n'):
        if line.strip():
            parts = line.split('\t')
            if len(parts) >= 4:
                chapters_data.append({
                    'id': int(parts[0]),
                    'chapter_number': int(parts[1]),
                    'last_updated': parts[2],
                    'default_language': parts[3]
                })
    
    # Parse chapter translations - handle tab-separated values carefully
    translations_data = []
    for line_num, line in enumerate(translations_match.group(1).strip().split('\n')):
        if line.strip():
            try:
                # Split by tabs, but handle content that may contain tabs
                parts = line.split('\t')
                if len(parts) >= 7:  # Minimum required fields
                    translations_data.append({
                        'id': int(parts[0]),
                        'chapter_id': int(parts[1]),
                        'language': parts[2],
                        'title': parts[3],
                        'content': parts[4],
                        'summary': parts[5] if parts[5] and parts[5] != '\\N' else parts[3],  # Use title if summary is null
                        'last_updated': parts[6],
                        'theme': parts[7] if len(parts) > 7 else 'temple'
                    })
            except (ValueError, IndexError) as e:
                print(f"Warning: Skipping line {line_num + 1} due to parsing error: {e}")
                continue
    
    return {
        'chapters': chapters_data,
        'translations': translations_data
    }

def convert_to_mongodb_format(parsed_data: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Convert parsed data to MongoDB chapter format"""
    
    chapters = parsed_data['chapters']
    translations = parsed_data['translations']
    
    # Group translations by chapter_id
    translations_by_chapter = {}
    for trans in translations:
        chapter_id = trans['chapter_id']
        if chapter_id not in translations_by_chapter:
            translations_by_chapter[chapter_id] = {}
        translations_by_chapter[chapter_id][trans['language']] = trans
    
    # Convert to MongoDB format
    mongodb_chapters = []
    
    for chapter in chapters:
        chapter_id = chapter['id']
        chapter_translations = translations_by_chapter.get(chapter_id, {})
        
        # Build multilingual dictionaries
        titles = {}
        contents = {}
        summaries = {}
        
        for lang, trans in chapter_translations.items():
            # Map language codes
            lang_key = lang
            if lang == 'english':
                lang_key = 'english'
            elif lang == 'hindi':
                lang_key = 'hindi'
            elif lang == 'telugu':
                lang_key = 'telugu'
            elif lang == 'marathi':
                lang_key = 'marathi'
            
            # Clean up content by removing excessive \n characters and formatting properly
            def clean_text(text):
                if not text:
                    return text
                
                # First, convert literal \n sequences to actual newlines
                text = text.replace('\\n', '\n')
                
                # Replace multiple consecutive newlines with double newlines (paragraph breaks)
                text = re.sub(r'\n{3,}', '\n\n', text)  # Replace 3+ \n with 2 \n
                
                # Replace single newlines with spaces, but keep double newlines as paragraph breaks
                text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)  # Replace single \n with space, keep double \n
                
                # Clean up multiple spaces
                text = re.sub(r' +', ' ', text)  # Replace multiple spaces with single space
                
                # Clean up any remaining formatting issues
                text = text.strip()
                
                return text
            
            titles[lang_key] = clean_text(trans['title'])
            contents[lang_key] = clean_text(trans['content'])
            summaries[lang_key] = clean_text(trans['summary'] if trans['summary'] else trans['title'])
        
        # Only include chapters that have at least English content
        if 'english' in titles:
            mongodb_chapter = {
                'number': chapter['chapter_number'],
                'title': titles,
                'content': contents,
                'summary': summaries,
                'created_at': datetime.utcnow(),
                'updated_at': datetime.utcnow()
            }
            mongodb_chapters.append(mongodb_chapter)
    
    # Sort by chapter number
    mongodb_chapters.sort(key=lambda x: x['number'])
    
    return mongodb_chapters

def generate_sample_quotes() -> List[Dict[str, Any]]:
    """Generate sample spiritual quotes"""
    quotes = [
        {
            "content": {
                "english": "Allah Malik - God is the Master of all",
                "hindi": "अल्लाह मालिक - ईश्वर सभी के स्वामी हैं",
                "telugu": "అల్లా మాలిక్ - దేవుడు అందరికీ యజమాని",
                "marathi": "अल्लाह मालिक - देव सर्वांचा स्वामी आहे"
            },
            "author": "Sai Baba",
            "date": datetime.utcnow(),
            "is_active": True
        },
        {
            "content": {
                "english": "Why fear when I am here?",
                "hindi": "डर किस बात का जब मैं यहाँ हूँ?",
                "telugu": "నేను ఇక్కడ ఉన్నప్పుడు ఎందుకు భయం?",
                "marathi": "मी इथे असताना भीती कशाची?"
            },
            "author": "Sai Baba",
            "date": datetime.utcnow(),
            "is_active": True
        },
        {
            "content": {
                "english": "Love all, serve all, help ever, hurt never",
                "hindi": "सबसे प्रेम करो, सबकी सेवा करो, हमेशा सहायता करो, कभी नुकसान न करो",
                "telugu": "అందరినీ ప్రేమించండి, అందరికీ సేవ చేయండి, ఎల్లప్పుడూ సహాయం చేయండి, ఎప్పుడూ హాని చేయవద్దు",
                "marathi": "सर्वांवर प्रेम करा, सर्वांची सेवा करा, नेहमी मदत करा, कधीही दुखावू नका"
            },
            "author": "Sai Baba",
            "date": datetime.utcnow(),
            "is_active": True
        },
        {
            "content": {
                "english": "He has no beginning, no end. He is the Doer, the Cause of everything. He is the Creator, the Protector and the Destroyer",
                "hindi": "उसका कोई आदि नहीं, कोई अंत नहीं। वह कर्ता है, सब कुछ का कारण है। वह सृष्टिकर्ता, पालनकर्ता और संहारकर्ता है",
                "telugu": "అతనికి ఆది లేదు, అంతు లేదు. అతను కర్త, ప్రతిదానికీ కారణం. అతను సృష్టికర్త, పాలకుడు మరియు సంహారకుడు",
                "marathi": "त्याची सुरुवात नाही, शेवट नाही. तो कर्ता आहे, प्रत्येकगोष्टीचे कारण आहे. तो निर्माता, संरक्षक आणि संहारक आहे"
            },
            "author": "Sai Baba",
            "date": datetime.utcnow(),
            "is_active": True
        },
        {
            "content": {
                "english": "God is One, though His names are many. You may call Him by any name, Allah, Hari, Brahma or Paramatma",
                "hindi": "भगवान एक है, यद्यपि उसके नाम अनेक हैं। आप उसे किसी भी नाम से पुकार सकते हैं, अल्लाह, हरि, ब्रह्म या परमात्मा",
                "telugu": "దేవుడు ఒకడే, అయినప్పటికీ అతని పేర్లు అనేకం. మీరు అతనిని ఏ పేరుతో అయినా పిలవవచ్చు, అల్లా, హరి, ब्रह्म లేదా పరమాత్మ",
                "marathi": "देव एकच आहे, जरी त्याची नावे अनेक आहेत. तुम्ही त्याला कोणत्याही नावाने हाक मारू शकता, अल्लाह, हरी, ब्रह्म किंवा परमात्मा"
            },
            "author": "Sai Baba",
            "date": datetime.utcnow(),
            "is_active": True
        }
    ]
    return quotes

if __name__ == "__main__":
    # Parse the backup file
    print("Parsing PostgreSQL backup data...")
    parsed_data = parse_copy_data('/app/backend/backup_chapters.sql')
    
    print(f"Found {len(parsed_data['chapters'])} chapters")
    print(f"Found {len(parsed_data['translations'])} translations")
    
    # Convert to MongoDB format
    print("Converting to MongoDB format...")
    mongodb_chapters = convert_to_mongodb_format(parsed_data)
    
    print(f"Generated {len(mongodb_chapters)} MongoDB chapters")
    
    # Print first chapter as sample
    if mongodb_chapters:
        first_chapter = mongodb_chapters[0]
        print(f"\nSample chapter {first_chapter['number']}:")
        print(f"Title (English): {first_chapter['title'].get('english', 'N/A')}")
        print(f"Languages available: {list(first_chapter['title'].keys())}")
        print(f"Content length: {len(first_chapter['content'].get('english', ''))}")
    
    # Generate sample quotes
    quotes = generate_sample_quotes()
    print(f"\nGenerated {len(quotes)} spiritual quotes")
    
    print("\nConversion completed successfully!")