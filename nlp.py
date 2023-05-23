from natasha import (
    Segmenter,
    MorphVocab,

    NewsEmbedding,
    NewsMorphTagger,
    Doc,
    AddrExtractor,
)

__segmentor = Segmenter()
__embedding = NewsEmbedding()
__tagger = NewsMorphTagger(__embedding)
__morpher = MorphVocab()
__address_extractor = AddrExtractor(__morpher)

def tokenize_text(text):
    text = text.strip()
    doc = Doc(text)
    doc.segment(__segmentor)
    doc.tag_morph(__tagger)
    for token in doc.tokens:
        token.lemmatize(__morpher)
    return doc

def iter_address_parts(text):
    matches = __address_extractor.find(text)
    invalid_types = ['площадь']
    if matches:
        for i in matches.fact.parts:
            t = i.type
            v = i.value
            if v and t and t not in invalid_types:
                if not v.isdigit() and len(v) < 3:
                    continue
                yield t, v