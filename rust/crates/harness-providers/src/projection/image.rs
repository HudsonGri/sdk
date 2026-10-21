use crate::{Adapter, invalid};
use acyclic_harness::{
    Result,
    model::{NativeMediaIntent, NativeMediaPolicy},
};
use serde_json::{Value, json};

pub(crate) fn part(
    adapter: Adapter,
    media: &str,
    policy: &NativeMediaPolicy,
    data: String,
) -> Result<Value> {
    let NativeMediaIntent::Image { detail } = policy.intent else {
        return Err(invalid("native file is not an image"));
    };
    let detail = serde_json::to_value(detail).map_err(|_| invalid("invalid image detail"))?;
    Ok(match adapter {
        Adapter::Responses => json!({"type":"message","role":"user","content":[{
            "type":"input_image","image_url":format!("data:{media};base64,{data}"),"detail":detail}]}),
        Adapter::Chat => json!({"type":"image_url","image_url":{
            "url":format!("data:{media};base64,{data}"),"detail":detail}}),
        Adapter::Anthropic => {
            json!({"type":"image","source":{"type":"base64","media_type":media,"data":data}})
        }
        Adapter::Gemini => json!({"inlineData":{"mimeType":media,"data":data}}),
    })
}

#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn assert_roundtrip(input: &[u8]) {
    use acyclic_harness::model::{ImageDetail, image_media_type};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    let formats = [
        b"\x89PNG\r\n\x1a\n".as_slice(),
        b"\xff\xd8\xff",
        b"GIF89a",
        b"RIFF0000WEBP",
    ];
    for header in formats {
        let bytes = [header, input].concat();
        let media = image_media_type(&bytes).unwrap();
        let policy = NativeMediaPolicy {
            intent: NativeMediaIntent::Image {
                detail: ImageDetail::Auto,
            },
            maximum_bytes: bytes.len() as u64,
            maximum_work: u32::MAX as u64,
            configuration: None,
        };
        for adapter in [
            Adapter::Responses,
            Adapter::Chat,
            Adapter::Anthropic,
            Adapter::Gemini,
        ] {
            let image = part(adapter, media, &policy, STANDARD.encode(&bytes)).unwrap();
            let encoded = serde_json::to_vec(&image).unwrap();
            let replay: Value = serde_json::from_slice(&encoded).unwrap();
            assert_eq!(image, replay);
            let data = match adapter {
                Adapter::Responses => {
                    image["content"][0]["image_url"]
                        .as_str()
                        .unwrap()
                        .split_once(',')
                        .unwrap()
                        .1
                }
                Adapter::Chat => {
                    image["image_url"]["url"]
                        .as_str()
                        .unwrap()
                        .split_once(',')
                        .unwrap()
                        .1
                }
                Adapter::Anthropic => image["source"]["data"].as_str().unwrap(),
                Adapter::Gemini => image["inlineData"]["data"].as_str().unwrap(),
            };
            assert_eq!(STANDARD.decode(data).unwrap(), bytes);
        }
    }
    // Recognition is total and never trusts a filename or an out-of-bounds header.
    let _ = image_media_type(input);
}
