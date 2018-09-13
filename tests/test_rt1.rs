extern crate hebb;

use hebb::experimental::rt1::*;

#[test]
fn test_rt1_add() {
  // TODO
  let x1 = constant_op(1.0_f32);
  let x2 = constant_op(2.0_f32);
  let y = add_op(x1.clone(), x2);
  let t = txn();
  x1.force_eval(t);
  y.force_eval(t);
  //println!("DEBUG: y: {:?}", y.get(t));
  panic!();
}

#[test]
fn test_rt1_switch() {
  // TODO
  let c = constant_op(true);
  let x1 = constant_op(1.0_f32);
  let x2 = constant_op(2.0_f32);
  let y = switch_op(c.clone(), x1.clone(), x2);
  let t = txn();
  x1.force_eval(t);
  y.force_eval(t);
  //println!("DEBUG: y: {:?}", y.get(t));
  panic!();
}
