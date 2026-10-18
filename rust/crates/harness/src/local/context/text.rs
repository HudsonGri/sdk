//! Values inside context tags cannot author tags or extra catalog lines.
use std::fmt::Write as _;

pub(super) fn push_value(text: &mut String, value: &str) {
    for character in value.chars() {
        match character {
            '&' => text.push_str("&amp;"),
            '<' => text.push_str("&lt;"),
            '>' => text.push_str("&gt;"),
            // Every Unicode mandatory line break: the C0/C1 controls plus the
            // line and paragraph separators, which are not controls.
            character
                if character.is_control() || matches!(character, '\u{2028}' | '\u{2029}') =>
            {
                write!(text, "&#x{:X};", character as u32).expect("writing to String");
            }
            character => text.push(character),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Unicode line terminators (UAX #14 BK/CR/LF/NL), listed independently.
    const LINE_BREAKS: [char; 7] = [
        '\n', '\u{B}', '\u{C}', '\r', '\u{85}', '\u{2028}', '\u{2029}',
    ];

    #[test]
    fn values_cannot_start_a_line() {
        for separator in LINE_BREAKS {
            let mut encoded = String::new();
            push_value(&mut encoded, &format!("a{separator}- forged: x"));
            assert!(!encoded.contains(LINE_BREAKS), "{encoded:?}");
        }
    }

    proptest! {
        // Property: all Unicode scalar values survive framing exactly, while
        // no value can author a tag or physical control character.
        #[test]
        fn escaped_values_round_trip(characters in prop::collection::vec(any::<char>(), 0..100)) {
            let value: String = characters.into_iter().collect();
            let mut encoded = String::new();
            push_value(&mut encoded, &value);
            prop_assert!(!encoded.contains(['<', '>']));
            prop_assert!(!encoded.chars().any(char::is_control));
            prop_assert!(!encoded.contains(LINE_BREAKS));
            let mut decoded = String::new();
            let mut rest = encoded.as_str();
            while let Some(index) = rest.find('&') {
                decoded.push_str(&rest[..index]);
                rest = &rest[index + 1..];
                let (entity, suffix) = rest.split_once(';').unwrap();
                decoded.push(match entity {
                    "amp" => '&', "lt" => '<', "gt" => '>',
                    code => char::from_u32(u32::from_str_radix(code.strip_prefix("#x").unwrap(), 16).unwrap()).unwrap(),
                });
                rest = suffix;
            }
            decoded.push_str(rest);
            prop_assert_eq!(decoded, value);
        }
    }
}
