use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::any::{Any};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap};
use std::marker::{PhantomData};
use std::rc::{Rc};
use std::sync::{Arc};

lazy_static! {
  static ref DEFAULT_CFG:   Mutex<DefaultConfig> = Mutex::new(DefaultConfig::new());
  static ref DEFAULT_CTX:   Mutex<Option<DefaultCtx>> = Mutex::new(None);
}

thread_local! {
  static UID:   Cell<u64> = Cell::new(0);
  static HEAP:  RefCell<THeap> = RefCell::new(THeap::new_root());
  static CTXS:  RefCell<Vec<Rc<dyn ExecutionCtx>>> = RefCell::new(Vec::new());
}

#[derive(Clone)]
pub struct DefaultConfig {
  pub default_opt_hint: Option<OptimizeHint>,
}

impl DefaultConfig {
  fn new() -> DefaultConfig {
    // TODO: read env vars.
    DefaultConfig{
      default_opt_hint: None,
    }
  }
}

fn default_ctx() -> impl ExecutionCtx {
  let mut ctx = DEFAULT_CTX.lock();
  if ctx.is_none() {
    *ctx = Some(DefaultCtx::default());
  }
  (*ctx).clone().unwrap()
}

pub fn thread_ctx() -> Rc<dyn ExecutionCtx> {
  CTXS.with(|ctxs| {
    let mut ctxs = ctxs.borrow_mut();
    if ctxs.is_empty() {
      // If there is no context, create a `DefaultCtx`.
      ctxs.push(Rc::new(default_ctx()));
    }
    let ctx = ctxs.last().unwrap().clone();
    ctx
  })
}

pub trait ExecutionCtx {
}

pub type DefaultCtx = DummyCtx;

#[derive(Clone, Default)]
pub struct DummyCtx {
}

impl ExecutionCtx for DummyCtx {
  #[cfg(feature = "gpu")] fn maybe_gpu(&self) -> Option<GPUDeviceCtx> { None }
  #[cfg(feature = "gpu")] fn maybe_multi_gpu(&self) -> Option<MultiGPUDeviceCtx> { None }

  #[cfg(feature = "gpu")]
  fn gpu(&self) -> GPUDeviceCtx {
    match self.maybe_gpu() {
      None => panic!("no GPU device ctx"),
      Some(ctx) => ctx,
    }
  }

  #[cfg(feature = "gpu")]
  fn multi_gpu(&self) -> MultiGPUDeviceCtx {
    match self.maybe_multi_gpu() {
      None => panic!("no multi-GPU device ctx"),
      Some(ctx) => ctx,
    }
  }
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

pub struct Txn(u64);

pub fn txn() -> Txn {
  Txn(next_uid())
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct STag {
  uid:      u64,
}

impl STag {
  fn new() -> STag {
    STag{uid: next_uid()}
  }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RTag {
  uid:      u64,
}

impl RTag {
  fn new() -> RTag {
    RTag{uid: next_uid()}
  }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Tag {
  stable:   STag,
  retain:   RTag,
}

#[derive(Clone)]
pub struct TagVec {
  inner:    Vec<Tag>,
}

impl TagVec {
  pub fn optimize(&self) -> FrameRef {
    self.optimize_with_hint(OptimizeHint::default())
  }

  pub fn optimize_with_hint<Hint: Into<OptimizeHint>>(&self, hint: Hint) -> FrameRef {
    let hint = hint.into();
    // TODO
    unimplemented!();
  }
}

#[derive(Clone, Debug)]
pub struct OptimizeHint {
}

impl Default for OptimizeHint {
  fn default() -> OptimizeHint {
    let cfg = DEFAULT_CFG.lock();
    if let Some(ref hint) = cfg.default_opt_hint {
      return hint.clone();
    }
    OptimizeHint::empty()
  }
}

impl OptimizeHint {
  pub fn empty() -> OptimizeHint {
    OptimizeHint{}
  }
}

#[derive(Clone, Copy)]
pub enum HeapObjKind {
  THeap,
  RWData,
  Thunk,
}

pub trait HeapObj: Any {
  fn _obj_kind(&self) -> HeapObjKind;
}

pub trait Placement {
}

pub struct FrameRef<'scope> {
  // TODO: reference to the frame heap, which can also be in the global heap.
  stable:   STag,
  up:       STag,
  _mrk:     PhantomData<&'scope ()>,
}

pub struct THeap {
  stable:   STag,
  objs:     HashMap<STag, Rc<dyn HeapObj>>,
}

impl THeap {
  pub fn new() -> THeap {
    THeap{
      stable:   STag::new(),
      objs:     HashMap::new(),
    }
  }

  fn new_root() -> THeap {
    let heap = THeap::new();
    assert_eq!(1, heap.stable.uid);
    heap
  }
}

impl HeapObj for THeap {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::THeap
  }
}

pub struct LDataRef<V> {
  stable:   STag,
  _mrk:     PhantomData<V>,
}

pub struct RWData<V> {
  // TODO
  stable:   STag,
  synccell: Arc<RwLock<RWDataCell<V>>>,
  code:     RWDataCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V: 'static> HeapObj for RWData<V> {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::RWData
  }
}

pub struct RWDataCell<V> {
  payload:  Option<V>,
}

pub struct RWDataCode<V> {
  alloc:    Option<Arc<Fn(Txn) -> V>>,
}

/*pub trait RWDataPlacement {
}*/

pub struct ThunkRef<V> {
  ref_:     Tag,
  _mrk:     PhantomData<V>,
}

/*pub trait ThunkRefExt<V> {
  fn get(&self, txn: Txn);
}

pub trait ThunkRefStackExt<V> {
  fn get(&self, txn: Txn, frame: ());
}

impl<V> ThunkRef<V> {
  pub fn _test(&self, txn: Txn) {
    self.get(txn);
    self.get(txn, ());
  }
}

impl<V> ThunkRefExt<V> for ThunkRef<V> {
  fn get(&self, txn: Txn) {
  }
}

impl<V> ThunkRefStackExt<V> for ThunkRef<V> {
  fn get(&self, txn: Txn, frame: ()) {
  }
}*/

pub struct Thunk<V> {
  stable:   STag,
  data:     Option<STag>,
  freevars: Vec<Tag>,
  code:     ThunkCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V: 'static> HeapObj for Thunk<V> {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::Thunk
  }
}

pub struct ThunkCode<V> {
  // TODO
  alloc:    Option<Arc<Fn(Txn) -> V>>,
  entry:    Option<Box<Fn(Txn, LDataRef<V>) -> bool>>,
  adjoint:  Option<Box<Fn(Pass, ThunkRef<V>, &mut Sink)>>,
}

/*pub trait ThunkPlacement {
}*/

pub struct Sink {
}
