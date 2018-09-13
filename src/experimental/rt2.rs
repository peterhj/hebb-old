use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::any::{Any};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug};
use std::marker::{PhantomData};
use std::ops::{Add};
use std::rc::{Rc};
use std::sync::{Arc};

thread_local! {
  static UID:       Cell<u64> = Cell::new(0);
  static H_MACH:    RefCell<HMach> = RefCell::new(unimplemented!());
}

fn next_uid() -> u64 {
  UID.with(|uid| {
    let prev_u = uid.get();
    let next_u = prev_u + 1;
    assert!(next_u != 0);
    uid.set(next_u);
    next_u
  })
}

pub struct Pass(u64);

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Txn(u64);

pub fn txn() -> Txn {
  Txn(next_uid())
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub struct Sym {
  u:        String,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct STag {
  uid:      u64,
}

impl STag {
  fn new() -> STag {
    STag{uid: next_uid()}
  }

  /*// FIXME
  pub fn lookup::<T: Any + Clone + 'static>(&self) -> T {
    let obj = HEAP.with(|heap| {
      let mut heap = heap.borrow_mut();
      heap.objs[self].clone()
    });
    if let Some(ref obj) = (&*obj).downcast_ref::<T>() {
      // TODO
      unimplemented!();
    } else {
      panic!();
    }
  }*/
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct RTag {
  uid:      u64,
}

impl RTag {
  fn new() -> RTag {
    RTag{uid: next_uid()}
  }

  pub fn _clone_exact(&self) -> RTag {
    RTag{uid: self.uid}
  }
}

#[derive(PartialEq, Eq, Hash, Debug)]
pub struct Tag {
  stable:   STag,
  retain:   RTag,
}

impl Tag {
  pub fn new(stable: STag) -> Tag {
    Tag{
      stable:   stable,
      retain:   RTag::new(),
    }
  }

  pub fn clone_ref(&self) -> Tag {
    Tag{
      stable:   self.stable,
      retain:   RTag::new(),
    }
  }

  pub fn _clone_exact(&self) -> Tag {
    Tag{
      stable:   self.stable,
      retain:   self.retain._clone_exact(),
    }
  }
}

pub trait Placement {
}

pub fn h_step() {
  H_MACH.with(|mach| {
    let mut mach = mach.borrow_mut();
    mach.step();
  })
}

pub struct HMach {
  inst:     HInst,
  regfile:  HRegfile,
  stack:    HStack,
  heap:     HHeap,
}

impl HMach {
  pub fn step(&mut self) {
    let &mut HMach{
      ref mut inst,
      ref mut regfile,
      ref mut stack,
      ref mut heap} = self;
    match inst.curr_expr.clone() {
      None => panic!(),
      Some(HExpr::OpApply{op, args}) => {
        match stack.frames.last() {
          Some(&HStackFrame::Update{ret, thunk}) => {
            let dst = STag::new(); // FIXME: lookup from thunk.
            match heap.objs.get(&op) {
              Some(&HObj::Op(ref op_obj)) => {
                // TODO
                if let Some(ref apply) = op_obj.code.apply {
                  (apply)(dst, args.clone());
                } else {
                  panic!();
                }
              }
              _ => panic!(),
            }
          }
          _ => panic!(),
        }
      }
      _ => unimplemented!(),
    }
  }
}

#[derive(Clone)]
pub enum HExpr {
  // TODO
  Atom{obj: STag},
  OpApply{op: STag, args: Vec<STag>},
}

pub struct HInst {
  curr_expr:    Option<HExpr>,
}

pub enum HValue {
  Addr(STag),
  Data(Box<dyn Any>),
}

pub struct HRegCell {
  // TODO
  //curr_txn:     Option<Txn>,
  l_consumers:  Mutex<HashSet<RTag>>,
  d_consumers:  HashSet<RTag>,
  l_sproducers: HashSet<STag>,
  d_sproducers: HashSet<STag>,
  l_producers:  HashSet<Tag>,
  d_producers:  HashSet<Tag>,
  // TODO: where/how to type this?
  value:        Option<HValue>,
}

pub struct HReg {
  cell:     Rc<RefCell<HRegCell>>,
  //cell:     Arc<RwLock<HRegCell>>,
}

pub struct HRegfile {
  regs:     HashMap<STag, HReg>,
}

pub enum HStackFrame {
  Update{ret: STag, thunk: STag},
}

pub struct HStack {
  frames:   Vec<HStackFrame>,
}

pub enum HObj {
  Fun(HFun),
  Op(HOp),
  Thunk(HThunk),
}

pub struct HHeap {
  objs:     HashMap<STag, HObj>,
}

pub struct HFun {
  // TODO
  stable:   STag,
}

/*pub struct HOpMode {
  Assign,
  Accumulate,
}*/

pub struct HOpCode {
  // TODO
  pub apply:    Option<Arc<Fn(/*Txn,*/ STag, Vec<STag>/*, HOpMode*/)>>,
  pub adjoint:  Option<Arc<Fn(Pass, STag, &mut Sink)>>,
}

pub struct HOp {
  // TODO
  stable:   STag,
  code:     HOpCode,
  //plc:      _,
}

pub struct HThunkCode {
  // TODO
  pub entry:    Option<Arc<Fn(Txn, Option<STag>) -> STag>>,
  pub adjoint:  Option<Arc<Fn(Pass, STag, &mut Sink)>>,
}

/*pub struct HThunkRef {
  tag:      Tag,
}

impl Clone for HThunkRef {
  fn clone(&self) -> HThunkRef {
    HThunkRef{
      tag:      self.tag.clone_ref(),
    }
  }
}*/

#[derive(Clone, Copy)]
pub enum HThunkStatus {
  Empty,
  BlackHole,
  Updated,
}

pub struct HThunk {
  stable:   STag,
  status:   HThunkStatus,
  dst:      Option<STag>,
  freevars: Vec<Tag>,
  code:     HThunkCode,
  //plc:      Option<Rc<dyn Placement>>,
}

pub struct Sink {
}
