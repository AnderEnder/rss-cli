//! Recover only direct RSS2 enclosures lost by feed-rs's media merge.
use quick_xml::{NsReader, encoding::DecodingReader, events::Event, name::ResolveResult};
use url::Url;

use crate::model::Enclosure;

pub(super) fn read(body: &[u8]) -> Option<Vec<Vec<Enclosure>>> {
    let mut reader = NsReader::from_reader(DecodingReader::new(body));
    reader.config_mut().expand_empty_elements = true;
    reader.config_mut().trim_markup_names_in_closing_tags = true;
    let mut buffer = Vec::new();
    let mut path = Vec::new();
    let mut bases: Vec<Option<Url>> = Vec::new();
    let mut items: Vec<Vec<Enclosure>> = Vec::new();
    let mut found_channel = false;
    let mut has_media = false;
    loop {
        match reader.read_resolved_event_into(&mut buffer).ok()? {
            (namespace, Event::Start(element)) => {
                let rss = matches!(namespace, ResolveResult::Unbound)
                    || matches!(namespace, ResolveResult::Bound(ns) if ns.as_ref() == "http://purl.org/rss/1.0/");
                let local = element.local_name();
                if path.len() == 3
                    && path.last() == Some(&true)
                    && matches!(namespace, ResolveResult::Bound(ns) if ns.as_ref() == "http://search.yahoo.com/mrss/")
                    && matches!(local.as_ref(), "content" | "group")
                {
                    has_media = true;
                }
                let expected = match path.len() {
                    0 => "rss",
                    1 => "channel",
                    2 => "item",
                    3 => "enclosure",
                    _ => "",
                };
                let mut relevant =
                    rss && local.as_ref() == expected && path.last().copied().unwrap_or(true);
                if relevant && path.len() == 1 {
                    relevant = !found_channel;
                    found_channel = true;
                }
                let mut base = bases.last().cloned().flatten();
                let mut url = None;
                let mut mime = None;
                let mut length = None;
                for attribute in element.attributes().flatten() {
                    let key = attribute.key.as_ref();
                    if key != "xml:base"
                        && !(relevant
                            && path.len() == 3
                            && matches!(key, "url" | "type" | "length"))
                    {
                        continue;
                    }
                    // feed-rs keeps the original value when XML escapes are malformed.
                    let value = quick_xml::escape::unescape(&attribute.value)
                        .unwrap_or_else(|_| attribute.value.clone());
                    match key {
                        "xml:base" => {
                            base = Url::parse(&value)
                                .ok()
                                .or_else(|| base.as_ref().and_then(|base| base.join(&value).ok()))
                                .or(base);
                        }
                        "url" => url = Some(value.into_owned()),
                        "type" => {
                            mime = value
                                .parse::<mediatype::MediaTypeBuf>()
                                .ok()
                                .map(|m| m.to_string());
                        }
                        "length" => length = value.parse().ok(),
                        _ => {}
                    }
                }
                if relevant && path.len() == 2 {
                    items.push(Vec::new());
                    has_media = false;
                }
                if relevant
                    && path.len() == 3
                    && let Some(url) = url.and_then(|url| {
                        Url::parse(&url)
                            .ok()
                            .or_else(|| base.as_ref().and_then(|base| base.join(&url).ok()))
                    })
                {
                    items.last_mut()?.push(Enclosure {
                        url: url.to_string(),
                        mime,
                        length,
                    });
                }
                path.push(relevant);
                bases.push(base);
            }
            (_, Event::End(_)) => {
                // Single-source RSS enclosures keep feed-rs's metadata validation.
                if path.len() == 3 && path.last() == Some(&true) && !has_media {
                    items.last_mut()?.clear();
                }
                path.pop()?;
                bases.pop()?;
            }
            (_, Event::Eof) => return Some(items),
            _ => {}
        }
        buffer.clear();
    }
}
