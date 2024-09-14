mod dispatch;
#[cfg(feature = "rolling_window_by")]
mod rolling_kernels;

use arrow::array::{ArrayRef, PrimitiveArray};
use arrow::legacy::kernels::rolling;
pub use dispatch::*;
use polars_core::prelude::*;
#[cfg(feature = "serde")]
use serde::{ser::SerializeStruct, Deserialize, Deserializer, Serialize, Serializer};

use crate::prelude::*;

#[derive(Clone, Debug)]
pub struct RollingOptionsDynamicWindow {
    /// The length of the window.
    pub window_size: Duration,
    /// Amount of elements in the window that should be filled before computing a result.
    pub min_periods: usize,
    /// Which side windows should be closed.
    pub closed_window: ClosedWindow,
    /// Optional parameters for the rolling function
    pub fn_params: DynArgs,
}

#[cfg(feature = "serde")]
impl Serialize for RollingOptionsDynamicWindow {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let rolling_fn_params = RollingFnParams::from_dyn_args(&self.fn_params);
        let mut state = serializer.serialize_struct("RollingOptionsDynamicWindow", 4)?;

        state.serialize_field("window_size", &self.window_size)?;
        state.serialize_field("min_periods", &self.min_periods)?;
        state.serialize_field("closed_window", &self.closed_window)?;
        state.serialize_field("fn_params", &rolling_fn_params)?;

        state.end()
    }
}

#[cfg(feature = "serde")]
impl<'de> Deserialize<'de> for RollingOptionsDynamicWindow {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            window_size: Duration,
            min_periods: usize,
            closed_window: ClosedWindow,
            #[serde(default)]
            fn_params: Option<RollingFnParams>,
        }

        let helper = Helper::deserialize(deserializer)?;
        let fn_params = helper
            .fn_params
            .as_ref()
            .and_then(|param| param.to_dyn_args());

        Ok(RollingOptionsDynamicWindow {
            window_size: helper.window_size,
            min_periods: helper.min_periods,
            closed_window: helper.closed_window,
            fn_params,
        })
    }
}

#[cfg(feature = "rolling_window_by")]
impl PartialEq for RollingOptionsDynamicWindow {
    fn eq(&self, other: &Self) -> bool {
        self.window_size == other.window_size
            && self.min_periods == other.min_periods
            && self.closed_window == other.closed_window
            && RollingFnParams::from_dyn_args(&self.fn_params)
                == RollingFnParams::from_dyn_args(&other.fn_params)
    }
}
