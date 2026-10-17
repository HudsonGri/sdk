#![cfg(feature = "tui")]
use acyclic_harness_tui::{
    TranscriptScrollbar,
    config::ThemeStyles,
    ratatui::{buffer::Buffer, layout::Rect, widgets::Widget},
};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    #[test]
    fn scrollbar_geometry_is_bounded_monotone_and_reaches_both_ends(maximum in any::<usize>(),height in 2u16..64,row in any::<u16>(),top in any::<usize>()) {
        let area=Rect::new(0,0,1,height);
        let at=TranscriptScrollbar::row_at(area,row,maximum);
        prop_assert!(at<=maximum);
        prop_assert_eq!(TranscriptScrollbar::row_at(area,0,maximum),0);
        prop_assert_eq!(TranscriptScrollbar::row_at(area,height-1,maximum),maximum);
        prop_assert!(TranscriptScrollbar::row_at(area,row.saturating_add(1),maximum)>=at);
        let mut buffer=Buffer::empty(area);
        TranscriptScrollbar{top,maximum,theme:&ThemeStyles::default()}.render(area,&mut buffer);
        let thumb=buffer.content.iter().filter(|cell|cell.symbol()=="█").count();
        prop_assert!(thumb<=usize::from(height));
        prop_assert_eq!(thumb==0,maximum==0);
    }
}

#[test]
#[should_panic(expected = "position must use wide multiplication")]
fn overflowing_row_product_is_detected_negative_control() {
    let maximum = usize::MAX;
    let expected = TranscriptScrollbar::row_at(Rect::new(0, 0, 1, 20), 19, maximum);
    let broken = 19usize.wrapping_mul(maximum) / 19;
    assert_eq!(broken, expected, "position must use wide multiplication");
}
