#[derive(Debug, Clone, Copy)]
pub struct Temperature {
    pub min: i32,
    pub max: i32,
    pub sum: i32,
    pub count: u32,
}

impl Temperature {
    #[inline]
    pub fn new(temperature: i32) -> Self {
        Self {
            min: temperature,
            max: temperature,
            sum: temperature,
            count: 1,
        }
    }

    #[inline]
    pub fn update(&mut self, other: &Temperature) {
        self.min = i32::min(self.min, other.min);
        self.max = i32::max(self.max, other.max);
        self.sum += other.sum;
        self.count += other.count;
    }

    #[inline]
    pub fn update_single(&mut self, temperature: i32) {
        self.min = i32::min(self.min, temperature);
        self.max = i32::max(self.max, temperature);
        self.sum += temperature;
        self.count += 1;
    }
}
