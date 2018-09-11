use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use std::any::{Any};
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug};
use std::marker::{PhantomData};
use std::ops::{Add};
use std::rc::{Rc};
use std::sync::{Arc};

lazy_static! {
  static ref DEFAULT_CFG:   Mutex<DefaultConfig> = Mutex::new(DefaultConfig::new());
  static ref DEFAULT_CTX:   Mutex<Option<DefaultCtx>> = Mutex::new(None);
}

thread_local! {
  static UID:   Cell<u64> = Cell::new(0);
  static HEAP:  RefCell<Heap> = RefCell::new(Heap::new_root());
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
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

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct RTag {
  uid:      u64,
}

impl RTag {
  fn new() -> RTag {
    RTag{uid: next_uid()}
  }
}

#[derive(PartialEq, Eq, Hash)]
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
      retain:   self.retain,
    }
  }
}

//#[derive(Clone)]
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
  Heap,
  Data,
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

pub struct Heap {
  stable:   STag,
  objs:     HashMap<STag, Rc<dyn Any>>,
  //objs:     HashMap<STag, Rc<dyn HeapObj>>,
}

impl Heap {
  pub fn new() -> Heap {
    Heap{
      stable:   STag::new(),
      objs:     HashMap::new(),
    }
  }

  fn new_root() -> Heap {
    Heap{
      stable:   STag{uid: 0},
      objs:     HashMap::new(),
    }
  }
}

impl HeapObj for Heap {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::Heap
  }
}

pub struct LDataRef<V> {
  // TODO: do we need a retain tag here?
  stable:   STag,
  //tag:      Tag,
  _mrk:     PhantomData<V>,
}

impl<V> LDataRef<V> {
  pub fn _from_stag(stable: STag) -> LDataRef<V> {
    LDataRef{
      stable:   stable,
      _mrk:     PhantomData,
    }
  }
}

impl<V: 'static> LDataRef<V> {
  pub fn _get_obj(&self) -> LData<V> {
    let data_obj = HEAP.with(|heap| {
      let mut heap = heap.borrow_mut();
      heap.objs[&self.stable].clone()
    });
    if let Some(ref data) = (&*data_obj).downcast_ref::<Data<V>>() {
      let cloned_data = data._clone_exact();
      LData{
        stable:     self.stable,
        synccell:   cloned_data.synccell,
        code:       cloned_data.code,
        plc:        cloned_data.plc,
      }
    } else {
      panic!();
    }
  }
}

pub struct LData<V> {
  stable:   STag,
  synccell: Arc<RwLock<DataCell<V>>>,
  code:     DataCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V> LData<V> {
  /*pub fn get(&self, txn: Txn) -> RwLockReadGuard<V> {
    // TODO
    unimplemented!();
  }*/

  pub fn get_mut(&self, txn: Txn) -> RwLockWriteGuard<V> {
    /*// TODO: want to avoid forcing an eval here;
    // see the commented out datastate condition below.
    //self.eval(txn);
    match self.synccell.write() {
      None => panic!("RThunk: get: no data"),
      Some(ref data) => {
        match self.state.get() {
          ThunkState::Empty => {
            self.state.set(ThunkState::BlackHole);
            self.eval(txn);
            self.state.set(ThunkState::Valid);
          }
          ThunkState::BlackHole => {
            panic!();
          }
          ThunkState::Valid => {
          }
        }
        data._get_mut(txn)
      }
    }*/
    let cell = self.synccell.write();
    RwLockWriteGuard::map(cell, |cell| {
      if cell.payload.is_none() {
        cell.payload = match self.code.alloc {
          None => panic!("Data: get_mut: missing alloc"),
          Some(ref alloc) => Some((alloc)(txn)),
        };
      }
      cell.payload.as_mut().unwrap()
    })
  }
}

pub struct Data<V> {
  // TODO
  stable:   STag,
  synccell: Arc<RwLock<DataCell<V>>>,
  code:     DataCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V: 'static> Data<V> {
  pub fn _put_obj(self) -> STag {
    HEAP.with(|heap| {
      let stable = self.stable;
      //let retain = RTag::new();
      let mut heap = heap.borrow_mut();
      heap.objs.insert(stable, Rc::new(self));
      /*ThunkRef{
        tag:    Tag{stable, retain},
        _mrk:   PhantomData,
      }*/
      stable
    })
  }
}

impl<V> Data<V> {
  pub fn new(code: DataCode<V>) -> Data<V> {
    Data{
      // TODO
      stable:   STag::new(),
      synccell: Arc::new(RwLock::new(Default::default())),
      code:     code,
      plc:      None,
    }
  }

  pub fn _clone_exact(&self) -> Data<V> {
    Data{
      stable:   self.stable,
      synccell: self.synccell.clone(),
      code:     self.code.clone(),
      plc:      self.plc.clone(),
    }
  }

  pub fn _get(&self, txn: Txn) -> RwLockReadGuard<V> {
    let cell = self.synccell.read();
    RwLockReadGuard::map(cell, |cell| match cell.payload {
      None => panic!("Data: get: missing payload"),
      Some(ref payload) => payload,
    })
  }

  pub fn _get_mut(&self, txn: Txn) -> RwLockWriteGuard<V> {
    let cell = self.synccell.write();
    RwLockWriteGuard::map(cell, |cell| {
      if cell.payload.is_none() {
        cell.payload = match self.code.alloc {
          None => panic!("Data: get_mut: missing alloc"),
          Some(ref alloc) => Some((alloc)(txn)),
        };
      }
      cell.payload.as_mut().unwrap()
    })
  }
}

impl<V: 'static> HeapObj for Data<V> {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::Data
  }
}

pub struct DataCell<V> {
  curr_txn:     Option<Txn>,
  l_consumers:  Mutex<HashSet<RTag>>,
  d_consumers:  HashSet<RTag>,
  l_sproducers: HashSet<STag>,
  d_sproducers: HashSet<STag>,
  l_producers:  HashSet<Tag>,
  d_producers:  HashSet<Tag>,
  payload:      Option<V>,
}

impl<V> Default for DataCell<V> {
  fn default() -> Self {
    DataCell{
      curr_txn:     None,
      l_consumers:  Mutex::new(HashSet::new()),
      d_consumers:  HashSet::new(),
      l_sproducers: HashSet::new(),
      d_sproducers: HashSet::new(),
      l_producers:  HashSet::new(),
      d_producers:  HashSet::new(),
      payload:      None,
    }
  }
}

pub struct DataCode<V> {
  alloc:    Option<Arc<Fn(Txn) -> V>>,
}

impl<V> Clone for DataCode<V> {
  fn clone(&self) -> DataCode<V> {
    DataCode{
      alloc:    self.alloc.clone(),
    }
  }
}

/*pub trait DataPlacement {
}*/

pub struct ThunkRef<V> {
  tag:      Tag,
  _mrk:     PhantomData<V>,
}

impl<V> Clone for ThunkRef<V> {
  fn clone(&self) -> ThunkRef<V> {
    ThunkRef{
      tag:      self.tag.clone_ref(),
      _mrk:     PhantomData,
    }
  }
}

impl<V> ThunkRef<V> {
  pub fn _from_tag(tag: Tag) -> ThunkRef<V> {
    ThunkRef{
      tag:      tag,
      _mrk:     PhantomData,
    }
  }

  pub fn _clone_exact(&self) -> ThunkRef<V> {
    ThunkRef{
      tag:      self.tag._clone_exact(),
      _mrk:     PhantomData,
    }
  }
}

impl<V: 'static> ThunkRef<V> {
  pub fn _get_obj(&self) -> RThunk<V> {
    let thunk_obj = HEAP.with(|heap| {
      let mut heap = heap.borrow_mut();
      heap.objs[&self.tag.stable].clone()
    });
    if let Some(ref thunk) = (&*thunk_obj).downcast_ref::<Thunk<V>>() {
      let cloned_thunk = thunk._clone_exact();
      let cloned_data = match cloned_thunk.data {
        None => None,
        Some(s) => {
          let data_obj = HEAP.with(|heap| {
            let mut heap = heap.borrow_mut();
            heap.objs[&s].clone()
          });
          if let Some(ref data) = (&*data_obj).downcast_ref::<Data<V>>() {
            let cloned_data = data._clone_exact();
            Some(cloned_data)
          } else {
            panic!();
          }
        }
      };
      RThunk{
        tag:        self.tag._clone_exact(),
        data:       cloned_data,
        state:      cloned_thunk.state,
        freevars:   cloned_thunk.freevars,
        code:       cloned_thunk.code,
        plc:        cloned_thunk.plc,
      }
    } else {
      panic!();
    }
  }

  pub fn force_eval(&self, txn: Txn) {
    let obj = HEAP.with(|heap| {
      let heap = heap.borrow();
      heap.objs[&self.tag.stable].clone()
    });
    if let Some(ref thunk) = (&*obj).downcast_ref::<Thunk<V>>() {
      println!("ThunkRef: force_eval: success");
      thunk._force_eval(txn);
    } else {
      panic!();
    }
  }

  /*pub fn get(&self, txn: Txn) -> V {
    // TODO: want to avoid strictly forcing an eval here,
    // i.e. the following kind of line:
    //      /*self.eval(txn);*/
    match self.state.get() {
      ThunkState::Empty => {
        self.state.set(ThunkState::BlackHole);
        self.force_eval(txn);
        self.state.set(ThunkState::Valid);
      }
      ThunkState::BlackHole => {
        panic!();
      }
      ThunkState::Valid => {
      }
    }
    // FIXME
    unimplemented!();
  }*/
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ThunkState {
  Empty,
  BlackHole,
  Valid,
}

pub struct RThunk<V> {
  tag:      Tag,
  data:     Option<Data<V>>,
  state:    Rc<Cell<ThunkState>>,
  freevars: Vec<Tag>,
  code:     ThunkCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V: 'static> RThunk<V> {
  pub fn force_eval(&self, txn: Txn) {
    let thunkref = ThunkRef::<V>::_from_tag(self.tag._clone_exact());
    thunkref.force_eval(txn);
  }

  pub fn get(&self, txn: Txn) -> RwLockReadGuard<V> {
    // TODO: want to avoid strictly forcing an eval here,
    // i.e. the following kind of line:
    //      /*self.eval(txn);*/
    match self.data {
      None => panic!("RThunk: get: no data"),
      Some(ref data) => {
        match self.state.get() {
          ThunkState::Empty => {
            println!("RThunk: get: force eval...");
            self.force_eval(txn);
          }
          ThunkState::BlackHole => {
            panic!();
          }
          ThunkState::Valid => {
            println!("RThunk: get: already valid");
          }
        }
        assert_eq!(ThunkState::Valid, self.state.get());
        data._get(txn)
      }
    }
  }
}

pub struct Thunk<V> {
  stable:   STag,
  data:     Option<STag>,
  state:    Rc<Cell<ThunkState>>,
  freevars: Vec<Tag>,
  code:     ThunkCode<V>,
  plc:      Option<Rc<dyn Placement>>,
}

impl<V> Thunk<V> {
  pub fn _clone_exact(&self) -> Thunk<V> {
    Thunk{
      stable:   self.stable,
      data:     self.data,
      state:    self.state.clone(),
      freevars: self.freevars.iter().map(|v| v._clone_exact()).collect(),
      code:     self.code.clone(),
      plc:      self.plc.clone(),
    }
  }
}

impl<V: 'static> HeapObj for Thunk<V> {
  fn _obj_kind(&self) -> HeapObjKind {
    HeapObjKind::Thunk
  }
}

impl<V: 'static> Thunk<V> {
  pub fn _put_obj(self) -> ThunkRef<V> {
    HEAP.with(|heap| {
      let stable = self.stable;
      let retain = RTag::new();
      let mut heap = heap.borrow_mut();
      heap.objs.insert(stable, Rc::new(self));
      ThunkRef{
        tag:    Tag{stable, retain},
        _mrk:   PhantomData,
      }
    })
  }

  pub fn _force_eval(&self, txn: Txn) {
    match self.code.entry {
      None => panic!("Thunk: _force_eval: missing entry"),
      Some(ref entry) => {
        self.state.set(ThunkState::BlackHole);
        // TODO: For extra laziness, can pass `Option<LDataRef<V>>` to the
        // entry code, and turn into an object there.
        //
        // This might be necessary for thunks which do not mutate their "owned"
        // data and instead simply redirect to another thunk's data.
        let dataref = LDataRef::<V>::_from_stag(match self.data {
          None => panic!(),
          Some(stable) => stable,
        });
        let data = dataref._get_obj();
        (entry)(txn, data);
        self.state.set(ThunkState::Valid);
      }
    }
  }
}

pub struct ThunkCode<V> {
  // TODO
  //alloc:    Option<Arc<Fn(Txn) -> V>>,
  entry:    Option<Arc<Fn(Txn, LData<V>) -> bool>>,
  adjoint:  Option<Arc<Fn(Pass, ThunkRef<V>, &mut Sink)>>,
}

impl<V> Clone for ThunkCode<V> {
  fn clone(&self) -> ThunkCode<V> {
    ThunkCode{
      //alloc:    self.alloc.clone(),
      entry:    self.entry.clone(),
      adjoint:  self.adjoint.clone(),
    }
  }
}

/*pub trait ThunkPlacement {
}*/

pub struct Sink {
}

pub struct ConstantOp<V> {
  _mrk: PhantomData<V>,
}

impl<V: Clone + Debug + 'static> ConstantOp<V> {
  pub fn build_thunk(value: V) -> Thunk<V> {
    // TODO
    let stable = STag::new();
    let data = Data::new(DataCode{
      //alloc:    code.alloc.clone(),
      alloc:    {
        let v = value.clone();
        Some(Arc::new(move |_txn| {
          v.clone()
        }))
      },
    });
    let dataref = data._put_obj();
    let code = ThunkCode{
      entry:    {
        let value = value.clone();
        Some(Arc::new(move |txn, y| {
          // TODO: this should write something to `data`.
          println!("ConstantOp: entry");
          let mut y = y.get_mut(txn);
          *y = value.clone();
          println!("ConstantOp:   result: {:?}", *y);
          true
        }))
      },
      adjoint:  None,
    };
    Thunk{
      stable:   stable,
      data:     Some(dataref),
      state:    Rc::new(Cell::new(ThunkState::Empty)),
      freevars: Vec::new(),
      code:     code,
      plc:      None,
    }
  }
}

pub fn constant_op<V: Clone + Debug + 'static>(value: V) -> ThunkRef<V> {
  // TODO
  let thunk = ConstantOp::build_thunk(value);
  let thunkref = thunk._put_obj();
  thunkref
}

pub struct AddOp<V> {
  _mrk: PhantomData<V>,
}

impl<V: Add<Output=V> + Clone + Default + Debug + 'static> AddOp<V> {
  pub fn build_thunk(x1: ThunkRef<V>, x2: ThunkRef<V>) -> Thunk<V> {
    // TODO
    let stable = STag::new();
    let data = Data::new(DataCode{
      //alloc:    code.alloc.clone(),
      alloc:    Some(Arc::new(move |_txn| {
        // TODO
        V::default()
      })),
    });
    let dataref = data._put_obj();
    let code = ThunkCode{
      entry:    {
        /*let x1 = x1._clone_exact();
        let x2 = x2._clone_exact();*/
        let x1 = x1._get_obj();
        let x2 = x2._get_obj();
        Some(Arc::new(move |txn, y| {
          // TODO
          println!("AddOp: entry");
          let x1 = x1.get(txn);
          let x2 = x2.get(txn);
          let mut y = y.get_mut(txn);
          *y = x1.clone() + x2.clone();
          println!("AddOp:   result: {:?}", *y);
          true
        }))
      },
      adjoint:  None,
    };
    Thunk{
      stable:   stable,
      data:     Some(dataref),
      state:    Rc::new(Cell::new(ThunkState::Empty)),
      freevars: vec![x1.tag, x2.tag],
      code:     code,
      plc:      None,
    }
  }
}

pub fn add_op<V: Add<Output=V> + Clone + Default + Debug + 'static>(x1: ThunkRef<V>, x2: ThunkRef<V>) -> ThunkRef<V> {
  // TODO
  let thunk = AddOp::build_thunk(x1, x2);
  let thunkref = thunk._put_obj();
  thunkref
}

pub struct SwitchOp<V> {
  _mrk: PhantomData<V>,
}

impl<V: Clone + Default + Debug + 'static> SwitchOp<V> {
  pub fn build_thunk(cond: ThunkRef<bool>, x1: ThunkRef<V>, x2: ThunkRef<V>) -> Thunk<V> {
    // TODO
    let stable = STag::new();
    let data = Data::new(DataCode{
      //alloc:    code.alloc.clone(),
      alloc:    Some(Arc::new(move |_txn| {
        // TODO
        V::default()
      })),
    });
    let dataref = data._put_obj();
    let code = ThunkCode{
      entry:    {
        /*let cond = cond._clone_exact();
        let x1 = x1._clone_exact();
        let x2 = x2._clone_exact();*/
        let cond = cond._get_obj();
        let x1 = x1._get_obj();
        let x2 = x2._get_obj();
        Some(Arc::new(move |txn, y| {
          // TODO
          println!("SwitchOp: entry");
          let cond = cond.get(txn);
          let x1 = x1.get(txn);
          let x2 = x2.get(txn);
          let mut y = y.get_mut(txn);
          match *cond {
            false   => {
              *y = x1.clone();
            }
            true    => {
              *y = x2.clone();
            }
          }
          println!("SwitchOp:   result: {:?}", *y);
          true
        }))
      },
      adjoint:  None,
    };
    Thunk{
      stable:   stable,
      data:     Some(dataref),
      state:    Rc::new(Cell::new(ThunkState::Empty)),
      freevars: vec![x1.tag, x2.tag],
      code:     code,
      plc:      None,
    }
  }
}

pub fn switch_op<V: Clone + Default + Debug + 'static>(cond: ThunkRef<bool>, x1: ThunkRef<V>, x2: ThunkRef<V>) -> ThunkRef<V> {
  // TODO
  let thunk = SwitchOp::build_thunk(cond, x1, x2);
  let thunkref = thunk._put_obj();
  thunkref
}
