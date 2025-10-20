import requests
from pydantic import BaseModel, Field
from config import Config

from typing import Literal

def is_valid_url(url: str) -> bool:
    """
    Validates if a given string is a valid URL.

    Args:
        url (str): The URL string to validate.
    """
    if not url.startswith("http://") and not url.startswith("https://"):
        return False

    try:
        response = requests.get(url, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False


class SearchResponseFormat(BaseModel):
    web_url: str
    image_url: str


class SearchContentResponseFormat(SearchResponseFormat):
    headline: str = Field(default="Brief headline summary of the content")


class SearchBookResponseFormat(SearchResponseFormat):
    title: str


class SearchItemResponseFormat(SearchResponseFormat):
    name: str

class SearchVideoResponseFormat(SearchResponseFormat):
    title: str

class ContentReference(BaseModel):
    """Represents a content reference with name and type"""
    description: str
    type: Literal["book", "article", "video", "item", "other"]

class TranscriptReferencesFormat(BaseModel):
    """Structured format for references found in transcript"""
    organisations: list[str] = Field(default_factory=list)
    people: list[str] = Field(default_factory=list)
    content: list[ContentReference] = Field(default_factory=list)  # (name, type) tuples
    events: list[str] = Field(default_factory=list)

def search_perplexity(
    query: str,
    model: str = "sonar",
    return_images: bool = True,
    response_format: BaseModel | None = None,
    search_domain_filter: list[str] | None = None,
) -> dict:
    url = "https://api.perplexity.ai/chat/completions"
    headers = {
        "Authorization": f"Bearer {Config.PERPLEXITY_API_KEY}",
        "Content-Type": "application/json",
    }
    data = {
        "model": model,
        "media_response": {"enable_media_classifier": return_images},
        "messages": [{"role": "user", "content": query}],
    }
    if response_format:
        data["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "schema": response_format.model_json_schema(),
            },
        }
    if search_domain_filter:
        data["search_domain_filter"] = search_domain_filter

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error occurred: {e}")
        return {}


def search_person(name: str) -> SearchResponseFormat:
    query = f"""
    Return a DIRECT image file URL (must end in .jpg, .png, .jpeg, or .webp) and a wikipedia url for the person '{name}'.
    
    For image_url: Return a direct link to an image file, NOT a webpage. Example of correct format:
    - CORRECT: https://upload.wikimedia.org/wikipedia/commons/thumb/a/b/person.jpg
    - WRONG: https://commons.wikimedia.org/wiki/File:person.jpg (this is a webpage, not an image)
    
    If a wikipedia url is not found, return another relevant biographical url or personal website.
    Prefer Wikimedia Commons for images.
    
    Respond in using the format {str(SearchResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchResponseFormat,
        search_domain_filter=[
            "wikipedia.org",
            "wikimedia.org",
            "-pinterest.com",
            "-goodreads.com",
            "-gettyimages.com"
        ]
    )
    result = SearchResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    
    images = completion.get("images", [])

    if images:
        result.image_url = images[0].get("image_url", "")
    else:
        result.image_url = result.image_url if is_valid_url(result.image_url) else ""

    return result


def search_organisation(name: str) -> SearchResponseFormat:
    query = f"""
    Return a DIRECT logo image file URL (must end in .jpg, .png, .jpeg, .svg, or .webp) and a wikipedia url for the organisation '{name}'.
    
    For image_url: Return a direct link to an image file, NOT a webpage. 
    The URL must be a downloadable image file, not a Wikipedia page or category.
    
    If a wikipedia url is not found, return another relevant url.
    Prefer Wikimedia Commons or official websites for logo images.
    
    Respond in using the format {str(SearchResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchResponseFormat,
        search_domain_filter=[
            "wikipedia.org",
            "wikimedia.org",
            "-pinterest.com",
            "-goodreads.com"
        ]
    )
    result = SearchResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def search_twitter(
    text: str,
    author: str | None = None,
    date: str | None = None,
) -> str:
    """Returns a tweet url matching the text and author"""
    return ""


def search_book(title: str, author: str | None = None) -> SearchBookResponseFormat:
    query = f"""
    Return a source url and DIRECT image file URL for the book cover of '{title}'.
    The author is '{author or "unknown"}'.
    
    For image_url: Return a direct link to a book cover image file (must end in .jpg, .png, or .jpeg), NOT a webpage.
    Example: https://covers.openlibrary.org/b/id/12345-L.jpg
    
    Prefer Open Library (covers.openlibrary.org) or Archive.org for book cover images.
    Return a Wikipedia or official publisher url if available.

    Respond in using the format {str(SearchBookResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchBookResponseFormat,
        search_domain_filter=[
            "openlibrary.org",
            "archive.org",
            "wikipedia.org",
            "-goodreads.com",
            "-pinterest.com"
        ]
    )
    result = SearchBookResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def search_item(
    name: str,
    content_source: str | None = None,
) -> SearchItemResponseFormat:
    query = f"""
    Return a source url and DIRECT image file URL (must end in .jpg, .png, .jpeg, or .webp) for the item '{name}'.
    The content source is '{content_source or "unknown"}'.
    
    For image_url: Return a direct downloadable image file link, NOT a webpage or category page.
    Prefer Wikipedia or manufacturer official sites for images.

    Respond in using the format {str(SearchItemResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchItemResponseFormat,
        search_domain_filter=[
            "wikipedia.org",
            "wikimedia.org",
            "-pinterest.com",
            "-amazon.com"
        ]
    )
    result = SearchItemResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def search_video(
    description: str,
) -> SearchVideoResponseFormat:
    query = f"""
    Return a source url and image url for the video referencing '{description}'.
    Return a YouTube url if available, otherwise return another relevant url.
    For the image url, return a thumbnail or relevant image if available.
    Return title of the video as well.

    Respond in using the format {str(SearchVideoResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchVideoResponseFormat,
        search_domain_filter=[
            "youtube.com",
            "vimeo.com",
            "archive.org",
            "-pinterest.com"
        ]
    )
    result = SearchVideoResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def search_content(
    description: str,
    content_source: (
        str | None
    ) = None,
    date: str | None = None,
) -> SearchContentResponseFormat:
    query = f"""
    Return a source url and image url for the "content" referencing '{description}'.
    
    If content is 'article', return url from {content_source or "a reputable news site"}.
    If otherwise, return a relevant url.
    Prefer Wikimedia Commons or official sources for images.

    Respond in using the format {str(SearchContentResponseFormat.model_json_schema())}
    where title is a headline summary of the content.
    """
    completion = search_perplexity(
        query, 
        response_format=SearchContentResponseFormat,
        search_domain_filter=[
            "wikipedia.org",
            "wikimedia.org",
            "-pinterest.com",
            "-goodreads.com"
        ]
    )
    result = SearchContentResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def search_event(
    description: str,
    date: str | None = None,
) -> SearchResponseFormat:
    query = f"""
    Return a source url and image url for the event referencing '{description}'.
    The content date is '{date or "unknown"}'.
    Prefer Wikipedia or official event websites for images.

    Respond in using the format {str(SearchResponseFormat.model_json_schema())}
    """
    completion = search_perplexity(
        query, 
        response_format=SearchResponseFormat,
        search_domain_filter=[
            "wikipedia.org",
            "wikimedia.org",
            "-pinterest.com"
        ]
    )
    result = SearchResponseFormat.model_validate_json(
        completion["choices"][0]["message"]["content"]
    )
    return result


def extract_references_from_transcript(transcript: str) -> TranscriptReferencesFormat:
    """
    Extract references to organizations, people, content, and events from a transcript.
    
    Args:
        transcript: The transcript text to analyze
        
    Returns:
        TranscriptReferencesFormat with lists of found references
    """
    query = f"""
Read through the following transcript. Were references to any of the following made:
• famous organisations
• famous people
• pieces of content (e.g. books, letters, articles, food or drink, items)
• events

If references to any of the above were made in the provided transcript, structure your answer as an object with the keys: organisations, people, content, events. 

For each key, the value is a list of all references to that entity. If no references are made for that entity, an empty list should be returned for that key.

For content, return a list of objects where each object has "description" and "type" fields.

Here's an example response:
{{{{
  "organisations": [],
  "people": ["Alan Watts", "Richard Feynman"],
  "content": [{{"description": "The Wisdom of Insecurity", "type":"book"}}],
  "events": ["The Arab Spring"]
}}}}

If no references to any organisations, people, content, or events were made in the provided transcript, then return keys with empty lists as values.

Here's the transcript: {transcript}
"""
    try:
        print(f"[DEBUG] Calling Perplexity to extract references from: {transcript[:100]}...")
        completion = search_perplexity(query, response_format=TranscriptReferencesFormat)
        
        print(f"[DEBUG] Perplexity response: {completion}")
        
        if not completion or "choices" not in completion:
            print("[DEBUG] No choices in completion")
            return TranscriptReferencesFormat()
        
        content = completion["choices"][0]["message"]["content"]
        print(f"[DEBUG] Response content: {content}")
        
        result = TranscriptReferencesFormat.model_validate_json(content)
        print(f"[DEBUG] Parsed result - people: {result.people}, content: {result.content}, orgs: {result.organisations}, events: {result.events}")
        return result
        
    except Exception as e:
        print(f"[ERROR] Error extracting references: {e}")
        import traceback
        traceback.print_exc()
        return TranscriptReferencesFormat()

def process_transcript_references(transcript: str) -> dict:
    """
    Process transcript: extract references, then search for the FIRST one found.
    Priority: content → people → organisations → events
    
    Args:
        transcript: The transcript text to analyze
        
    Returns:
        dict with the first found reference and its search result
    """
    references = extract_references_from_transcript(transcript)
    
    result = {
        'people': [],
        'organisations': [],
        'content': [],
        'events': []
    }
    
    # Priority 1: Content (books, articles, videos, items)
    if references.content:
        content_ref = references.content[0]
        try:
            if content_ref.type == "book":
                search_result = search_book(content_ref.description)
            elif content_ref.type == "video":
                search_result = search_video(content_ref.description)
            elif content_ref.type == "item":
                search_result = search_item(content_ref.description)
            else:
                search_result = search_content(content_ref.description)
            
            result['content'].append({
                'description': content_ref.description,
                'type': content_ref.type,
                'web_url': search_result.web_url,
                'image_url': search_result.image_url
            })
            return result
        except Exception as e:
            print(f"Error searching content {content_ref.description}: {e}")
    
    # Priority 2: People
    if references.people:
        person = references.people[0]
        try:
            search_result = search_person(person)
            result['people'].append({
                'name': person,
                'web_url': search_result.web_url,
                'image_url': search_result.image_url
            })
            return result
        except Exception as e:
            print(f"Error searching person {person}: {e}")
    
    # Priority 3: Organisations
    if references.organisations:
        org = references.organisations[0]
        try:
            search_result = search_organisation(org)
            result['organisations'].append({
                'name': org,
                'web_url': search_result.web_url,
                'image_url': search_result.image_url
            })
            return result
        except Exception as e:
            print(f"Error searching organisation {org}: {e}")
    
    # Priority 4: Events
    if references.events:
        event = references.events[0]
        try:
            search_result = search_event(event)
            result['events'].append({
                'description': event,
                'web_url': search_result.web_url,
                'image_url': search_result.image_url
            })
            return result
        except Exception as e:
            print(f"Error searching event {event}: {e}")
    
    return result