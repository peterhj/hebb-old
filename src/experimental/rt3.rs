use std::any::{Any};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::{Entry};
use std::fmt::{Debug};
use std::marker::{PhantomData};
//use std::ops::{Add};
use std::rc::{Rc};
use std::sync::{Arc};

thread_local! {
  static MACH:  RefCell<Machine> = RefCell::new(unimplemented!());
}

pub enum LExpr {
}

pub struct Machine {
  uid:      u64,
  control:  MControl,
  regfile:  MRegfile,
  heap:     MHeap,
  stack:    MStack,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct MKey {
  addr: MAddr,
  sym:  Option<MSym>,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct MAddr {
  id:   u64,
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct MSym {
  u:    String,
}

#[derive(Clone)]
pub struct MLambdaForm {
  freevars: Vec<MSym>,
  args:     Vec<MSym>,
  update:   bool,
  expr:     Rc<MExpr>,
}

#[derive(Clone)]
pub struct MLit {
  opaque:   Rc<dyn Any>,
}

#[derive(Clone)]
pub struct MOp {
  //opaque:   Rc<dyn Any>,
  code:     MOpCode,
}

#[derive(Clone)]
pub struct MOpCode {
}

#[derive(Clone)]
pub enum MBindExpr {
  BindLit{key: MKey, lit: MLit},
}
/*pub struct MBindExpr {
  key:      MKey,
  lambda:   MLambdaForm,
}*/

#[derive(Clone)]
pub enum MExpr {
  Binds{exprs: Vec<MBindExpr>},
  Bind{key: MKey, lambda: MLambdaForm},
  //Bind{expr: MBindExpr},
  /*DefLit{key: MKey, lit: MLit, expr: Rc<MExpr>},
  DefOp{key: MKey, op: MOp, expr: Rc<MExpr>},*/
}

pub enum MInst {
  Eval{func: MSym, args: Vec<MSym>, local_env: MEnv},
  Enter{addr: MAddr},
}

pub struct MControl {
  next_expr:    Option<MExpr>,
}

pub struct MReg {
}

pub struct MRegfile {
}

pub enum MObj {
  Lit(MLit),
  Op(MOp),
}

pub struct MEnv {
  resolves: HashMap<MSym, MAddr>,
}

pub struct MHeap {
  env:  HashMap<MKey, MObj>,
  //contents: HashMap<MAddr, MClosure>,
}

impl MHeap {
  pub fn bind_lit(&mut self, key: MKey, lit: MLit) {
    match self.env.entry(key) {
      Entry::Occupied(_) => panic!(),
      Entry::Vacant(entry) => {
        entry.insert(MObj::Lit(lit));
      }
    }
  }

  pub fn bind_op(&mut self, key: MKey, op: MOp) {
    match self.env.entry(key) {
      Entry::Occupied(_) => panic!(),
      Entry::Vacant(entry) => {
        entry.insert(MObj::Op(op));
      }
    }
  }
}

pub struct MStack {
}

impl Machine {
  fn next_uid(&mut self) -> u64 {
    let prev_u = self.uid;
    let next_u = prev_u + 1;
    assert!(next_u != 0);
    self.uid = next_u;
    next_u
  }

  pub fn feed_expr(&mut self, expr: MExpr) {
    self.control.next_expr = Some(expr);
  }

  pub fn step(&mut self) {
    if self.control.next_expr.is_none() {
      panic!("Machine: no expr");
    }
    // TODO
    let curr_expr = self.control.next_expr.take().unwrap();
    let next_expr = match curr_expr {
      MExpr::Binds{exprs} => {
        // TODO
        unimplemented!();
      }
      /*MExpr::Bind{expr} => {
        match expr {
          MBindExpr::BindLit{key, lit} => {
            // TODO
            // TODO: bind into topmost heap from stack.
            //unimplemented!();
            self.heap.bind_lit(key, lit);
          }
        }
      }*/
      MExpr::Bind{key, lambda} => {
        // TODO
        unimplemented!();
      }
      /*MExpr::DefLit{key, lit, expr} => {
        // TODO: bind into topmost heap from stack.
        self.heap.bind_lit(key, lit);
        /*let heap = self.top_heap();
        heap.bind_lit(key, lit);*/
        (*expr).clone()
      }
      MExpr::DefOp{key, op, expr} => {
        // TODO: bind into topmost heap from stack.
        self.heap.bind_op(key, op);
        (*expr).clone()
      }*/
    };
    self.control.next_expr = Some(next_expr);
  }
}
