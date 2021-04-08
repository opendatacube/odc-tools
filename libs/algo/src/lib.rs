use pyo3::prelude::{pymodule, PyModule, PyResult, Python};
use numpy::{PyArray4, PyArray3, ToPyArray};
use ndarray::{array, Array, ArrayBase, ArrayView, ArrayViewMut, Axis, Ix1, Ix2, Ix3, Ix4};


fn geomedian<'a>(in_array: ArrayView<'a, f32, Ix4>, max_iter: usize, eps: f32) -> Array<f32, Ix3> {
    
    let shape = in_array.shape();
    let rows = shape[0];
    let columns = shape[1];
    let bands = shape[2];

    let mut out_array: Array<f32, Ix3> = ArrayBase::zeros([rows, columns, bands]);

    for row in 0..shape[0] {
        for column in 0..shape[1] {
            geomedian_pixel(in_array, &mut out_array, row, column, max_iter, eps);
        }
    }

    out_array
}


fn geomedian_pixel<'a>(
    in_array: ArrayView<'a, f32, Ix4>,
    out_array: &mut Array<f32, Ix3>,
    row: usize,
    column: usize,
    max_iter: usize, 
    eps: f32
) { 

    let bands = in_array.shape()[2];
    let time_steps = in_array.shape()[3];
    
    let mut valid_time_steps: Vec<usize> = Vec::with_capacity(time_steps);
    for t in 0..time_steps {
        if !in_array[[row, column, 0, t]].is_nan() {
            &valid_time_steps.push(t);
        }
    }
    
    for band in 0..bands {
        for &t in &valid_time_steps {
            out_array[[row, column, band]] += in_array[[row, column, band, t]];
        }

        out_array[[row, column, band]] /= valid_time_steps.len() as f32;
    }

    for it in 0..max_iter {
        let mut inv_dist_sum: f32 = 0.0;
        let mut temp_median: Vec<f32> = vec![0.0; bands];

        for &t in &valid_time_steps {
            
            let mut dist: f32 = 0.0;
            for band in 0..bands {
                dist += (out_array[[row, column, band]] - in_array[[row, column, band, t]]).powi(2); 
            }
            
            dist = dist.sqrt();
            
            let mut inv_dist = 1.0 / dist; 

            if dist == 0.0 {
                inv_dist = 0.0;
            }

            inv_dist_sum += inv_dist;

            for band in 0..bands {
                temp_median[band] += in_array[[row, column, band, t]] * inv_dist;
            }
        }

        let mut change: f32 = 0.0;
        for band in 0..bands {
            temp_median[band] /= inv_dist_sum;
            change += (temp_median[band] - out_array[[row, column, band]]).powi(2);
            out_array[[row, column, band]] = temp_median[band];
        }

        if change.sqrt() < eps {
            break;
        }

    }
    

}


#[pymodule]
fn backend(_py: Python, m: &PyModule) -> PyResult<()> {
    
    #[pyfn(m, "rust_geomedian")]
    fn py_geomedian<'a>(py: Python<'a>, in_array: &'a PyArray4<f32>, max_iter: usize, eps: f32) -> &'a PyArray3<f32> {
        let in_array = in_array.readonly();
        let in_array = in_array.as_array();
        geomedian(in_array, max_iter, eps).to_pyarray(py)
    }
    Ok(())
}
