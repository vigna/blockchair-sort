use std::time::Instant;
use voracious_radix_sort::{self, RadixSort};

struct Xoshiro256Plus {
    s: [u64; 4]
}

impl Xoshiro256Plus {

    fn next(&mut self) -> u64 {
    	let result_plus = self.s[0].wrapping_add(self.s[3]);
        let t = self.s[1] << 17;
        self.s[2] ^= self.s[0];
        self.s[3] ^= self.s[1];
        self.s[1] ^= self.s[2];
        self.s[0] ^= self.s[3];
        self.s[2] ^= t;
        self.s[3] = self.s[3].rotate_left(45);
        result_plus
    }
}

fn main() {
    let mut r = Xoshiro256Plus{s: [1, 2, 3, 4]};
    let mut v = vec![0u64; 2000000000];

    for _ in 0..10 {
        for x in &mut v {
            *x = r.next(); 
        }
        let start = Instant::now();
        v.voracious_mt_sort(10);
        println!("{}", Instant::now().duration_since(start).as_nanos() as f64 / 1E9);
    }

}
