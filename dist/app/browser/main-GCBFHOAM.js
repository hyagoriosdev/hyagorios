var Q_=Object.defineProperty,ex=Object.defineProperties;var tx=Object.getOwnPropertyDescriptors;var Op=Object.getOwnPropertySymbols;var nx=Object.prototype.hasOwnProperty,ix=Object.prototype.propertyIsEnumerable;var Fp=(n,e,t)=>e in n?Q_(n,e,{enumerable:!0,configurable:!0,writable:!0,value:t}):n[e]=t,Lt=(n,e)=>{for(var t in e||={})nx.call(e,t)&&Fp(n,t,e[t]);if(Op)for(var t of Op(e))ix.call(e,t)&&Fp(n,t,e[t]);return n},Xt=(n,e)=>ex(n,tx(e));var kt=null,la=!1,bu=1;var bi=Symbol("SIGNAL");function je(n){let e=kt;return kt=n,e}function ua(){return kt}var da={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function Tu(n){if(la)throw new Error("");if(kt===null)return;kt.consumerOnSignalRead(n);let e=kt.producersTail;if(e!==void 0&&e.producer===n)return;let t,i=kt.recomputing;if(i&&(t=e!==void 0?e.nextProducer:kt.producers,t!==void 0&&t.producer===n)){kt.producersTail=t,t.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===kt&&(!i||sx(r,kt)))return;let s=Fr(kt),o={producer:n,consumer:kt,nextProducer:t,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};kt.producersTail=o,e!==void 0?e.nextProducer=o:kt.producers=o,s&&zp(n,o)}function kp(){bu++}function Up(n){if(!(Fr(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===bu)){if(!n.producerMustRecompute(n)&&!ha(n)){Eu(n);return}n.producerRecomputeValue(n),Eu(n)}}function wu(n){if(n.consumers===void 0)return;let e=la;la=!0;try{for(let t=n.consumers;t!==void 0;t=t.nextConsumer){let i=t.consumer;i.dirty||rx(i)}}finally{la=e}}function Bp(){return kt?.consumerAllowSignalWrites!==!1}function rx(n){n.dirty=!0,wu(n),n.consumerMarkedDirty?.(n)}function Eu(n){n.dirty=!1,n.lastCleanEpoch=bu}function fa(n){return n&&Vp(n),je(n)}function Vp(n){n.producersTail=void 0,n.recomputing=!0}function Iu(n,e){je(e),n&&Hp(n)}function Hp(n){n.recomputing=!1;let e=n.producersTail,t=e!==void 0?e.nextProducer:n.producers;if(t!==void 0){if(Fr(n))do t=Cu(t);while(t!==void 0);e!==void 0?e.nextProducer=void 0:n.producers=void 0}}function ha(n){for(let e=n.producers;e!==void 0;e=e.nextProducer){let t=e.producer,i=e.lastReadVersion;if(i!==t.version||(Up(t),i!==t.version))return!0}return!1}function Bs(n){if(Fr(n)){let e=n.producers;for(;e!==void 0;)e=Cu(e)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function zp(n,e){let t=n.consumersTail,i=Fr(n);if(t!==void 0?(e.nextConsumer=t.nextConsumer,t.nextConsumer=e):(e.nextConsumer=void 0,n.consumers=e),e.prevConsumer=t,n.consumersTail=e,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)zp(r.producer,r)}function Cu(n){let e=n.producer,t=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:e.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(e.consumers=i,!Fr(e)){let s=e.producers;for(;s!==void 0;)s=Cu(s)}return t}function Fr(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function sx(n,e){let t=e.producersTail;if(t!==void 0){let i=e.producers;do{if(i===n)return!0;if(i===t)break;i=i.nextProducer}while(i!==void 0)}return!1}function Gp(n,e){return Object.is(n,e)}function ox(){throw new Error}var Wp=ox;function ax(n){Wp(n)}function Au(n){Wp=n}var cx=null;function Du(n,e){Bp()||ax(n),n.equal(n.value,e)||(n.value=e,lx(n))}var Ru=Xt(Lt({},da),{equal:Gp,value:void 0,kind:"signal"});function lx(n){n.version++,kp(),wu(n),cx?.(n)}function Yt(n){return typeof n=="function"}function pa(n){let t=n(i=>{Error.call(i),i.stack=new Error().stack});return t.prototype=Object.create(Error.prototype),t.prototype.constructor=t,t}var ma=pa(n=>function(t){n(this),this.message=t?`${t.length} errors occurred during unsubscription:
${t.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=t});function Vs(n,e){if(n){let t=n.indexOf(e);0<=t&&n.splice(t,1)}}var Gt=class n{constructor(e){this.initialTeardown=e,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let e;if(!this.closed){this.closed=!0;let{_parentage:t}=this;if(t)if(this._parentage=null,Array.isArray(t))for(let s of t)s.remove(this);else t.remove(this);let{initialTeardown:i}=this;if(Yt(i))try{i()}catch(s){e=s instanceof ma?s.errors:[s]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let s of r)try{jp(s)}catch(o){e=e??[],o instanceof ma?e=[...e,...o.errors]:e.push(o)}}if(e)throw new ma(e)}}add(e){var t;if(e&&e!==this)if(this.closed)jp(e);else{if(e instanceof n){if(e.closed||e._hasParent(this))return;e._addParent(this)}(this._finalizers=(t=this._finalizers)!==null&&t!==void 0?t:[]).push(e)}}_hasParent(e){let{_parentage:t}=this;return t===e||Array.isArray(t)&&t.includes(e)}_addParent(e){let{_parentage:t}=this;this._parentage=Array.isArray(t)?(t.push(e),t):t?[t,e]:e}_removeParent(e){let{_parentage:t}=this;t===e?this._parentage=null:Array.isArray(t)&&Vs(t,e)}remove(e){let{_finalizers:t}=this;t&&Vs(t,e),e instanceof n&&e._removeParent(this)}};Gt.EMPTY=(()=>{let n=new Gt;return n.closed=!0,n})();var Nu=Gt.EMPTY;function ga(n){return n instanceof Gt||n&&"closed"in n&&Yt(n.remove)&&Yt(n.add)&&Yt(n.unsubscribe)}function jp(n){Yt(n)?n():n.unsubscribe()}var Tn={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var kr={setTimeout(n,e,...t){let{delegate:i}=kr;return i?.setTimeout?i.setTimeout(n,e,...t):setTimeout(n,e,...t)},clearTimeout(n){let{delegate:e}=kr;return(e?.clearTimeout||clearTimeout)(n)},delegate:void 0};function $p(n){kr.setTimeout(()=>{let{onUnhandledError:e}=Tn;if(e)e(n);else throw n})}function Pu(){}var qp=Lu("C",void 0,void 0);function Xp(n){return Lu("E",void 0,n)}function Yp(n){return Lu("N",n,void 0)}function Lu(n,e,t){return{kind:n,value:e,error:t}}var Ji=null;function Ur(n){if(Tn.useDeprecatedSynchronousErrorHandling){let e=!Ji;if(e&&(Ji={errorThrown:!1,error:null}),n(),e){let{errorThrown:t,error:i}=Ji;if(Ji=null,t)throw i}}else n()}function Zp(n){Tn.useDeprecatedSynchronousErrorHandling&&Ji&&(Ji.errorThrown=!0,Ji.error=n)}var Ki=class extends Gt{constructor(e){super(),this.isStopped=!1,e?(this.destination=e,ga(e)&&e.add(this)):this.destination=fx}static create(e,t,i){return new Br(e,t,i)}next(e){this.isStopped?Fu(Yp(e),this):this._next(e)}error(e){this.isStopped?Fu(Xp(e),this):(this.isStopped=!0,this._error(e))}complete(){this.isStopped?Fu(qp,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(e){this.destination.next(e)}_error(e){try{this.destination.error(e)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},ux=Function.prototype.bind;function Ou(n,e){return ux.call(n,e)}var ku=class{constructor(e){this.partialObserver=e}next(e){let{partialObserver:t}=this;if(t.next)try{t.next(e)}catch(i){va(i)}}error(e){let{partialObserver:t}=this;if(t.error)try{t.error(e)}catch(i){va(i)}else va(e)}complete(){let{partialObserver:e}=this;if(e.complete)try{e.complete()}catch(t){va(t)}}},Br=class extends Ki{constructor(e,t,i){super();let r;if(Yt(e)||!e)r={next:e??void 0,error:t??void 0,complete:i??void 0};else{let s;this&&Tn.useDeprecatedNextContext?(s=Object.create(e),s.unsubscribe=()=>this.unsubscribe(),r={next:e.next&&Ou(e.next,s),error:e.error&&Ou(e.error,s),complete:e.complete&&Ou(e.complete,s)}):r=e}this.destination=new ku(r)}};function va(n){Tn.useDeprecatedSynchronousErrorHandling?Zp(n):$p(n)}function dx(n){throw n}function Fu(n,e){let{onStoppedNotification:t}=Tn;t&&kr.setTimeout(()=>t(n,e))}var fx={closed:!0,next:Pu,error:dx,complete:Pu};var Jp=typeof Symbol=="function"&&Symbol.observable||"@@observable";function Kp(n){return n}function Qp(n){return n.length===0?Kp:n.length===1?n[0]:function(t){return n.reduce((i,r)=>r(i),t)}}var Vr=(()=>{class n{constructor(t){t&&(this._subscribe=t)}lift(t){let i=new n;return i.source=this,i.operator=t,i}subscribe(t,i,r){let s=px(t)?t:new Br(t,i,r);return Ur(()=>{let{operator:o,source:a}=this;s.add(o?o.call(s,a):a?this._subscribe(s):this._trySubscribe(s))}),s}_trySubscribe(t){try{return this._subscribe(t)}catch(i){t.error(i)}}forEach(t,i){return i=em(i),new i((r,s)=>{let o=new Br({next:a=>{try{t(a)}catch(c){s(c),o.unsubscribe()}},error:s,complete:r});this.subscribe(o)})}_subscribe(t){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(t)}[Jp](){return this}pipe(...t){return Qp(t)(this)}toPromise(t){return t=em(t),new t((i,r)=>{let s;this.subscribe(o=>s=o,o=>r(o),()=>i(s))})}}return n.create=e=>new n(e),n})();function em(n){var e;return(e=n??Tn.Promise)!==null&&e!==void 0?e:Promise}function hx(n){return n&&Yt(n.next)&&Yt(n.error)&&Yt(n.complete)}function px(n){return n&&n instanceof Ki||hx(n)&&ga(n)}function mx(n){return Yt(n?.lift)}function tm(n){return e=>{if(mx(e))return e.lift(function(t){try{return n(t,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function nm(n,e,t,i,r){return new Uu(n,e,t,i,r)}var Uu=class extends Ki{constructor(e,t,i,r,s,o){super(e),this.onFinalize=s,this.shouldUnsubscribe=o,this._next=t?function(a){try{t(a)}catch(c){e.error(c)}}:super._next,this._error=r?function(a){try{r(a)}catch(c){e.error(c)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){e.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var e;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:t}=this;super.unsubscribe(),!t&&((e=this.onFinalize)===null||e===void 0||e.call(this))}}};var im=pa(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var ri=(()=>{class n extends Vr{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(t){let i=new ya(this,this);return i.operator=t,i}_throwIfClosed(){if(this.closed)throw new im}next(t){Ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(t)}})}error(t){Ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=t;let{observers:i}=this;for(;i.length;)i.shift().error(t)}})}complete(){Ur(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:t}=this;for(;t.length;)t.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var t;return((t=this.observers)===null||t===void 0?void 0:t.length)>0}_trySubscribe(t){return this._throwIfClosed(),super._trySubscribe(t)}_subscribe(t){return this._throwIfClosed(),this._checkFinalizedStatuses(t),this._innerSubscribe(t)}_innerSubscribe(t){let{hasError:i,isStopped:r,observers:s}=this;return i||r?Nu:(this.currentObservers=null,s.push(t),new Gt(()=>{this.currentObservers=null,Vs(s,t)}))}_checkFinalizedStatuses(t){let{hasError:i,thrownError:r,isStopped:s}=this;i?t.error(r):s&&t.complete()}asObservable(){let t=new Vr;return t.source=this,t}}return n.create=(e,t)=>new ya(e,t),n})(),ya=class extends ri{constructor(e,t){super(),this.destination=e,this.source=t}next(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.next)===null||i===void 0||i.call(t,e)}error(e){var t,i;(i=(t=this.destination)===null||t===void 0?void 0:t.error)===null||i===void 0||i.call(t,e)}complete(){var e,t;(t=(e=this.destination)===null||e===void 0?void 0:e.complete)===null||t===void 0||t.call(e)}_subscribe(e){var t,i;return(i=(t=this.source)===null||t===void 0?void 0:t.subscribe(e))!==null&&i!==void 0?i:Nu}};var Hs=class extends ri{constructor(e){super(),this._value=e}get value(){return this.getValue()}_subscribe(e){let t=super._subscribe(e);return!t.closed&&e.next(this._value),t}getValue(){let{hasError:e,thrownError:t,_value:i}=this;if(e)throw t;return this._throwIfClosed(),i}next(e){super.next(this._value=e)}};function Bu(n,e){return tm((t,i)=>{let r=0;t.subscribe(nm(i,s=>{i.next(n.call(e,s,r++))}))})}var Vu;function _a(){return Vu}function Hn(n){let e=Vu;return Vu=n,e}var rm=Symbol("NotFound");function Hr(n){return n===rm||n?.name==="\u0275NotFound"}var nd="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",Ge=class extends Error{code;constructor(e,t){super(id(e,t)),this.code=e}};function xx(n){return`NG0${Math.abs(n)}`}function id(n,e){return`${xx(n)}${e?": "+e:""}`}function ot(n){for(let e in n)if(n[e]===ot)return e;throw Error("")}function Ta(n,e){return n?e?`${n} ${e}`:n:e||""}var Mx=ot({__forward_ref__:ot});function wa(n){return n.__forward_ref__=wa,n}function rn(n){return lm(n)?n():n}function lm(n){return typeof n=="function"&&n.hasOwnProperty(Mx)&&n.__forward_ref__===wa}function Et(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function Ia(n){return Sx(n,Ca)}function Sx(n,e){return n.hasOwnProperty(e)&&n[e]||null}function Ex(n){let e=n?.[Ca]??null;return e||null}function zu(n){return n&&n.hasOwnProperty(Ma)?n[Ma]:null}var Ca=ot({\u0275prov:ot}),Ma=ot({\u0275inj:ot}),Ke=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(e,t){this._desc=e,this.\u0275prov=void 0,typeof t=="number"?this.__NG_ELEMENT_ID__=t:t!==void 0&&(this.\u0275prov=Et({token:this,providedIn:t.providedIn||"root",factory:t.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function rd(n){return n&&!!n.\u0275providers}var sd=ot({\u0275cmp:ot}),od=ot({\u0275dir:ot}),ad=ot({\u0275pipe:ot});var Gu=ot({\u0275fac:ot}),ir=ot({__NG_ELEMENT_ID__:ot}),sm=ot({__NG_ENV_ID__:ot});function rr(n){return ld(n,"@Component"),n[sd]||null}function cd(n){return ld(n,"@Directive"),n[od]||null}function um(n){return ld(n,"@Pipe"),n[ad]||null}function ld(n,e){if(n==null)throw new Ge(-919,!1)}var dm=ot({ngErrorCode:ot}),bx=ot({ngErrorMessage:ot}),Tx=ot({ngTokenPath:ot});function ud(n,e){return fm("",-200,e)}function Aa(n,e){throw new Ge(-201,!1)}function fm(n,e,t){let i=new Ge(e,n);return i[dm]=e,i[bx]=n,t&&(i[Tx]=t),i}function wx(n){return n[dm]}var Wu;function hm(){return Wu}function nn(n){let e=Wu;return Wu=n,e}function dd(n,e,t){let i=Ia(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(t&8)return null;if(e!==void 0)return e;Aa(n,"")}var Ix={},Qi=Ix,Cx="__NG_DI_FLAG__",ju=class{injector;constructor(e){this.injector=e}retrieve(e,t){let i=er(t)||0;try{return this.injector.get(e,i&8?null:Qi,i)}catch(r){if(Hr(r))return r;throw r}}};function Ax(n,e=0){let t=_a();if(t===void 0)throw new Ge(-203,!1);if(t===null)return dd(n,void 0,e);{let i=Dx(e),r=t.retrieve(n,i);if(Hr(r)){if(i.optional)return null;throw r}return r}}function at(n,e=0){return(hm()||Ax)(rn(n),e)}function $e(n,e){return at(n,er(e))}function er(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function Dx(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function $u(n){let e=[];for(let t=0;t<n.length;t++){let i=rn(n[t]);if(Array.isArray(i)){if(i.length===0)throw new Ge(900,!1);let r,s=0;for(let o=0;o<i.length;o++){let a=i[o],c=Rx(a);typeof c=="number"?c===-1?r=a.token:s|=c:r=a}e.push(at(r,s))}else e.push(at(i))}return e}function Rx(n){return n[Cx]}function Gr(n,e){let t=n.hasOwnProperty(Gu);return t?n[Gu]:null}function pm(n,e,t){if(n.length!==e.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],s=e[i];if(t&&(r=t(r),s=t(s)),s!==r)return!1}return!0}function mm(n){return n.flat(Number.POSITIVE_INFINITY)}function Da(n,e){n.forEach(t=>Array.isArray(t)?Da(t,e):e(t))}function fd(n,e,t){e>=n.length?n.push(t):n.splice(e,0,t)}function $s(n,e){return e>=n.length-1?n.pop():n.splice(e,1)[0]}function gm(n,e){let t=[];for(let i=0;i<n;i++)t.push(e);return t}function vm(n,e,t,i){let r=n.length;if(r==e)n.push(t,i);else if(r===1)n.push(i,n[0]),n[0]=t;else{for(r--,n.push(n[r-1],n[r]);r>e;){let s=r-2;n[r]=n[s],r--}n[e]=t,n[e+1]=i}}function Ra(n,e,t){let i=jr(n,e);return i>=0?n[i|1]=t:(i=~i,vm(n,i,e,t)),i}function Na(n,e){let t=jr(n,e);if(t>=0)return n[t|1]}function jr(n,e){return Nx(n,e,1)}function Nx(n,e,t){let i=0,r=n.length>>t;for(;r!==i;){let s=i+(r-i>>1),o=n[s<<t];if(e===o)return s<<t;o>e?r=s:i=s+1}return~(r<<t)}var sr={},zn=[],$r=new Ke(""),hd=new Ke("",-1),pd=new Ke(""),Gs=class{get(e,t=Qi){if(t===Qi){let r=fm("",-201);throw r.name="\u0275NotFound",r}return t}};function Pa(n){return{\u0275providers:n}}function ym(n){return Pa([{provide:$r,multi:!0,useValue:n}])}function _m(...n){return{\u0275providers:md(!0,n),\u0275fromNgModule:!0}}function md(n,...e){let t=[],i=new Set,r,s=o=>{t.push(o)};return Da(e,o=>{let a=o;Sa(a,s,[],i)&&(r||=[],r.push(a))}),r!==void 0&&xm(r,s),t}function xm(n,e){for(let t=0;t<n.length;t++){let{ngModule:i,providers:r}=n[t];gd(r,s=>{e(s,i)})}}function Sa(n,e,t,i){if(n=rn(n),!n)return!1;let r=null,s=zu(n),o=!s&&rr(n);if(!s&&!o){let c=n.ngModule;if(s=zu(c),s)r=c;else return!1}else{if(o&&!o.standalone)return!1;r=n}let a=i.has(r);if(o){if(a)return!1;if(i.add(r),o.dependencies){let c=typeof o.dependencies=="function"?o.dependencies():o.dependencies;for(let l of c)Sa(l,e,t,i)}}else if(s){if(s.imports!=null&&!a){i.add(r);let l;Da(s.imports,u=>{Sa(u,e,t,i)&&(l||=[],l.push(u))}),l!==void 0&&xm(l,e)}if(!a){let l=Gr(r)||(()=>new r);e({provide:r,useFactory:l,deps:zn},r),e({provide:pd,useValue:r,multi:!0},r),e({provide:$r,useValue:()=>at(r),multi:!0},r)}let c=s.providers;if(c!=null&&!a){let l=n;gd(c,u=>{e(u,l)})}}else return!1;return r!==n&&n.providers!==void 0}function gd(n,e){for(let t of n)rd(t)&&(t=t.\u0275providers),Array.isArray(t)?gd(t,e):e(t)}var Px=ot({provide:String,useValue:ot});function Mm(n){return n!==null&&typeof n=="object"&&Px in n}function Lx(n){return!!(n&&n.useExisting)}function Ox(n){return!!(n&&n.useFactory)}function Ea(n){return typeof n=="function"}var qs=new Ke(""),xa={},om={},Hu;function Xs(){return Hu===void 0&&(Hu=new Gs),Hu}var gn=class{},tr=class extends gn{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(e,t,i,r){super(),this.parent=t,this.source=i,this.scopes=r,Xu(e,o=>this.processProvider(o)),this.records.set(hd,zr(void 0,this)),r.has("environment")&&this.records.set(gn,zr(void 0,this));let s=this.records.get(qs);s!=null&&typeof s.value=="string"&&this.scopes.add(s.value),this.injectorDefTypes=new Set(this.get(pd,zn,{self:!0}))}retrieve(e,t){let i=er(t)||0;try{return this.get(e,Qi,i)}catch(r){if(Hr(r))return r;throw r}}destroy(){zs(this),this._destroyed=!0;let e=je(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let t=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of t)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),je(e)}}onDestroy(e){return zs(this),this._onDestroyHooks.push(e),()=>this.removeOnDestroy(e)}runInContext(e){zs(this);let t=Hn(this),i=nn(void 0),r;try{return e()}finally{Hn(t),nn(i)}}get(e,t=Qi,i){if(zs(this),e.hasOwnProperty(sm))return e[sm](this);let r=er(i),s,o=Hn(this),a=nn(void 0);try{if(!(r&4)){let l=this.records.get(e);if(l===void 0){let u=Vx(e)&&Ia(e);u&&this.injectableDefInScope(u)?l=zr(qu(e),xa):l=null,this.records.set(e,l)}if(l!=null)return this.hydrate(e,l,r)}let c=r&2?Xs():this.parent;return t=r&8&&t===Qi?null:t,c.get(e,t)}catch(c){let l=wx(c);throw l===-200||l===-201?new Ge(l,null):c}finally{nn(a),Hn(o)}}resolveInjectorInitializers(){let e=je(null),t=Hn(this),i=nn(void 0),r;try{let s=this.get($r,zn,{self:!0});for(let o of s)o()}finally{Hn(t),nn(i),je(e)}}toString(){return"R3Injector[...]"}processProvider(e){e=rn(e);let t=Ea(e)?e:rn(e&&e.provide),i=kx(e);if(!Ea(e)&&e.multi===!0){let r=this.records.get(t);r||(r=zr(void 0,xa,!0),r.factory=()=>$u(r.multi),this.records.set(t,r)),t=e,r.multi.push(e)}this.records.set(t,i)}hydrate(e,t,i){let r=je(null);try{if(t.value===om)throw ud("");return t.value===xa&&(t.value=om,t.value=t.factory(void 0,i)),typeof t.value=="object"&&t.value&&Bx(t.value)&&this._ngOnDestroyHooks.add(t.value),t.value}finally{je(r)}}injectableDefInScope(e){if(!e.providedIn)return!1;let t=rn(e.providedIn);return typeof t=="string"?t==="any"||this.scopes.has(t):this.injectorDefTypes.has(t)}removeOnDestroy(e){let t=this._onDestroyHooks.indexOf(e);t!==-1&&this._onDestroyHooks.splice(t,1)}};function qu(n){let e=Ia(n),t=e!==null?e.factory:Gr(n);if(t!==null)return t;if(n instanceof Ke)throw new Ge(-204,!1);if(n instanceof Function)return Fx(n);throw new Ge(-204,!1)}function Fx(n){if(n.length>0)throw new Ge(-204,!1);let t=Ex(n);return t!==null?()=>t.factory(n):()=>new n}function kx(n){if(Mm(n))return zr(void 0,n.useValue);{let e=Sm(n);return zr(e,xa)}}function Sm(n,e,t){let i;if(Ea(n)){let r=rn(n);return Gr(r)||qu(r)}else if(Mm(n))i=()=>rn(n.useValue);else if(Ox(n))i=()=>n.useFactory(...$u(n.deps||[]));else if(Lx(n))i=(r,s)=>at(rn(n.useExisting),s!==void 0&&s&8?8:void 0);else{let r=rn(n&&(n.useClass||n.provide));if(Ux(n))i=()=>new r(...$u(n.deps));else return Gr(r)||qu(r)}return i}function zs(n){if(n.destroyed)throw new Ge(-205,!1)}function zr(n,e,t=!1){return{factory:n,value:e,multi:t?[]:void 0}}function Ux(n){return!!n.deps}function Bx(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function Vx(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function Xu(n,e){for(let t of n)Array.isArray(t)?Xu(t,e):t&&rd(t)?Xu(t.\u0275providers,e):e(t)}function La(n,e){let t;n instanceof tr?(zs(n),t=n):t=new ju(n);let i,r=Hn(t),s=nn(void 0);try{return e()}finally{Hn(r),nn(s)}}function Em(){return hm()!==void 0||_a()!=null}var wn=0,Re=1,Le=2,Rt=3,vn=4,Wt=5,qr=6,Ys=7,sn=8,or=9,Gn=10,Ut=11,Xr=12,vd=13,Yr=14,on=15,wi=16,ar=17,Wn=18,Ii=19,yd=20,oi=21,Oa=22,Zs=23,an=24,Fa=25,Zr=26,Bt=27,bm=1;var Ci=7,Js=8,cr=9,jt=10;function Ai(n){return Array.isArray(n)&&typeof n[bm]=="object"}function In(n){return Array.isArray(n)&&n[bm]===!0}function _d(n){return(n.flags&4)!==0}function Jr(n){return n.componentOffset>-1}function xd(n){return(n.flags&1)===1}function lr(n){return!!n.template}function Kr(n){return(n[Le]&512)!==0}function ur(n){return(n[Le]&256)===256}var Md="svg",Tm="math";function Cn(n){for(;Array.isArray(n);)n=n[wn];return n}function wm(n,e){return Cn(e[n])}function Di(n,e){return Cn(e[n.index])}function Im(n,e){return n.data[e]}function Ri(n,e){let t=e[n];return Ai(t)?t:t[wn]}function Cm(n){return(n[Le]&4)===4}function ka(n){return(n[Le]&128)===128}function Am(n){return In(n[Rt])}function Qr(n,e){return e==null?null:n[e]}function Sd(n){n[ar]=0}function Ed(n){n[Le]&1024||(n[Le]|=1024,ka(n)&&Qs(n))}function Ks(n){return!!(n[Le]&9216||n[an]?.dirty)}function Ua(n){n[Gn].changeDetectionScheduler?.notify(8),n[Le]&64&&(n[Le]|=1024),Ks(n)&&Qs(n)}function Qs(n){n[Gn].changeDetectionScheduler?.notify(0);let e=Ti(n);for(;e!==null&&!(e[Le]&8192||(e[Le]|=8192,!ka(e)));)e=Ti(e)}function bd(n,e){if(ur(n))throw new Ge(911,!1);n[oi]===null&&(n[oi]=[]),n[oi].push(e)}function Dm(n,e){if(n[oi]===null)return;let t=n[oi].indexOf(e);t!==-1&&n[oi].splice(t,1)}function Ti(n){let e=n[Rt];return In(e)?e[Rt]:e}function Rm(n){return n[Ys]??=[]}function Nm(n){return n.cleanup??=[]}function Pm(n,e,t,i){let r=Rm(e);r.push(t),n.firstCreatePass&&Nm(n).push(i,r.length-1)}var Ye={lFrame:jm(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var Yu=!1;function Lm(){return Ye.lFrame.elementDepthCount}function Om(){Ye.lFrame.elementDepthCount++}function Td(){Ye.lFrame.elementDepthCount--}function Fm(){return Ye.bindingsEnabled}function wd(){return Ye.skipHydrationRootTNode!==null}function Id(n){return Ye.skipHydrationRootTNode===n}function Cd(){Ye.skipHydrationRootTNode=null}function Ct(){return Ye.lFrame.lView}function dr(){return Ye.lFrame.tView}function ci(){let n=Ad();for(;n!==null&&n.type===64;)n=n.parent;return n}function Ad(){return Ye.lFrame.currentTNode}function km(){let n=Ye.lFrame,e=n.currentTNode;return n.isParent?e:e.parent}function es(n,e){let t=Ye.lFrame;t.currentTNode=n,t.isParent=e}function Dd(){return Ye.lFrame.isParent}function Rd(){Ye.lFrame.isParent=!1}function Nd(){return Yu}function Pd(n){let e=Yu;return Yu=n,e}function Um(n){return Ye.lFrame.bindingIndex=n}function Bm(n){let e=Ye.lFrame,t=e.bindingIndex;return e.bindingIndex=e.bindingIndex+n,t}function Vm(){return Ye.lFrame.inI18n}function Hm(n,e){let t=Ye.lFrame;t.bindingIndex=t.bindingRootIndex=n,Ba(e)}function zm(){return Ye.lFrame.currentDirectiveIndex}function Ba(n){Ye.lFrame.currentDirectiveIndex=n}function Gm(n){let e=Ye.lFrame.currentDirectiveIndex;return e===-1?null:n[e]}function Ld(){return Ye.lFrame.currentQueryIndex}function Va(n){Ye.lFrame.currentQueryIndex=n}function Hx(n){let e=n[Re];return e.type===2?e.declTNode:e.type===1?n[Wt]:null}function Od(n,e,t){if(t&4){let r=e,s=n;for(;r=r.parent,r===null&&!(t&1);)if(r=Hx(s),r===null||(s=s[Yr],r.type&10))break;if(r===null)return!1;e=r,n=s}let i=Ye.lFrame=Wm();return i.currentTNode=e,i.lView=n,!0}function Ha(n){let e=Wm(),t=n[Re];Ye.lFrame=e,e.currentTNode=t.firstChild,e.lView=n,e.tView=t,e.contextLView=n,e.bindingIndex=t.bindingStartIndex,e.inI18n=!1}function Wm(){let n=Ye.lFrame,e=n===null?null:n.child;return e===null?jm(n):e}function jm(n){let e={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=e),e}function $m(){let n=Ye.lFrame;return Ye.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Fd=$m;function za(){let n=$m();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function eo(){return Ye.lFrame.selectedIndex}function Ni(n){Ye.lFrame.selectedIndex=n}function fr(){Ye.lFrame.currentNamespace=Md}function Ga(){zx()}function zx(){Ye.lFrame.currentNamespace=null}function kd(){return Ye.lFrame.currentNamespace}var qm=!0;function Wa(){return qm}function ja(n){qm=n}function Zu(n,e=null,t=null,i){let r=Xm(n,e,t,i);return r.resolveInjectorInitializers(),r}function Xm(n,e=null,t=null,i,r=new Set){let s=[t||zn,_m(n)],o;return new tr(s,e||Xs(),o||null,r)}var nr=class n{static THROW_IF_NOT_FOUND=Qi;static NULL=new Gs;static create(e,t){if(Array.isArray(e))return Zu({name:""},t,e,"");{let i=e.name??"";return Zu({name:i},e.parent,e.providers,i)}}static \u0275prov=Et({token:n,providedIn:"any",factory:()=>at(hd)});static __NG_ELEMENT_ID__=-1},yn=new Ke(""),to=(()=>{class n{static __NG_ELEMENT_ID__=Gx;static __NG_ENV_ID__=t=>t}return n})(),Ju=class extends to{_lView;constructor(e){super(),this._lView=e}get destroyed(){return ur(this._lView)}onDestroy(e){let t=this._lView;return bd(t,e),()=>Dm(t,e)}};function Gx(){return new Ju(Ct())}var Ym=!1,Zm=new Ke(""),ts=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Hs(!1);debugTaskTracker=$e(Zm,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new Vr(t=>{t.next(!1),t.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let t=this.taskId++;return this.pendingTasks.add(t),this.debugTaskTracker?.add(t),t}has(t){return this.pendingTasks.has(t)}remove(t){this.pendingTasks.delete(t),this.debugTaskTracker?.remove(t),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=Et({token:n,providedIn:"root",factory:()=>new n})}return n})(),Ku=class extends ri{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(e=!1){super(),this.__isAsync=e,Em()&&(this.destroyRef=$e(to,{optional:!0})??void 0,this.pendingTasks=$e(ts,{optional:!0})??void 0)}emit(e){let t=je(null);try{super.next(e)}finally{je(t)}}subscribe(e,t,i){let r=e,s=t||(()=>null),o=i;if(e&&typeof e=="object"){let c=e;r=c.next?.bind(c),s=c.error?.bind(c),o=c.complete?.bind(c)}this.__isAsync&&(s=this.wrapInTimeout(s),r&&(r=this.wrapInTimeout(r)),o&&(o=this.wrapInTimeout(o)));let a=super.subscribe({next:r,error:s,complete:o});return e instanceof Gt&&e.add(a),a}wrapInTimeout(e){return t=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{e(t)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},si=Ku;function ba(...n){}function Ud(n){let e,t;function i(){n=ba;try{t!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(t),e!==void 0&&clearTimeout(e)}catch{}}return e=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(t=requestAnimationFrame(()=>{n(),i()})),()=>i()}function Jm(n){return queueMicrotask(()=>n()),()=>{n=ba}}var Bd="isAngularZone",Ws=Bd+"_ID",Wx=0,Zt=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new si(!1);onMicrotaskEmpty=new si(!1);onStable=new si(!1);onError=new si(!1);constructor(e){let{enableLongStackTrace:t=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:s=Ym}=e;if(typeof Zone>"u")throw new Ge(908,!1);Zone.assertZonePatched();let o=this;o._nesting=0,o._outer=o._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(o._inner=o._inner.fork(new Zone.TaskTrackingZoneSpec)),t&&Zone.longStackTraceZoneSpec&&(o._inner=o._inner.fork(Zone.longStackTraceZoneSpec)),o.shouldCoalesceEventChangeDetection=!r&&i,o.shouldCoalesceRunChangeDetection=r,o.callbackScheduled=!1,o.scheduleInRootZone=s,qx(o)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(Bd)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new Ge(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new Ge(909,!1)}run(e,t,i){return this._inner.run(e,t,i)}runTask(e,t,i,r){let s=this._inner,o=s.scheduleEventTask("NgZoneEvent: "+r,e,jx,ba,ba);try{return s.runTask(o,t,i)}finally{s.cancelTask(o)}}runGuarded(e,t,i){return this._inner.runGuarded(e,t,i)}runOutsideAngular(e){return this._outer.run(e)}},jx={};function Vd(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function $x(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function e(){Ud(()=>{n.callbackScheduled=!1,Qu(n),n.isCheckStableRunning=!0,Vd(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{e()}):n._outer.run(()=>{e()}),Qu(n)}function qx(n){let e=()=>{$x(n)},t=Wx++;n._inner=n._inner.fork({name:"angular",properties:{[Bd]:!0,[Ws]:t,[Ws+t]:!0},onInvokeTask:(i,r,s,o,a,c)=>{if(Xx(c))return i.invokeTask(s,o,a,c);try{return am(n),i.invokeTask(s,o,a,c)}finally{(n.shouldCoalesceEventChangeDetection&&o.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&e(),cm(n)}},onInvoke:(i,r,s,o,a,c,l)=>{try{return am(n),i.invoke(s,o,a,c,l)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!Yx(c)&&e(),cm(n)}},onHasTask:(i,r,s,o)=>{i.hasTask(s,o),r===s&&(o.change=="microTask"?(n._hasPendingMicrotasks=o.microTask,Qu(n),Vd(n)):o.change=="macroTask"&&(n.hasPendingMacrotasks=o.macroTask))},onHandleError:(i,r,s,o)=>(i.handleError(s,o),n.runOutsideAngular(()=>n.onError.emit(o)),!1)})}function Qu(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function am(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function cm(n){n._nesting--,Vd(n)}var js=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new si;onMicrotaskEmpty=new si;onStable=new si;onError=new si;run(e,t,i){return e.apply(t,i)}runGuarded(e,t,i){return e.apply(t,i)}runOutsideAngular(e){return e()}runTask(e,t,i,r){return e.apply(t,i)}};function Xx(n){return Km(n,"__ignore_ng_zone__")}function Yx(n){return Km(n,"__scheduler_tick__")}function Km(n,e){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[e]===!0}var ai=class{_console=console;handleError(e){this._console.error("ERROR",e)}},ns=new Ke("",{factory:()=>{let n=$e(Zt),e=$e(gn),t;return i=>{n.runOutsideAngular(()=>{e.destroyed&&!t?setTimeout(()=>{throw i}):(t??=e.get(ai),t.handleError(i))})}}}),Qm={provide:$r,useValue:()=>{let n=$e(ai,{optional:!0})},multi:!0},Zx=new Ke("",{factory:()=>{let n=$e(yn).defaultView;if(!n)return;let e=$e(ns),t=s=>{e(s.reason),s.preventDefault()},i=s=>{s.error?e(s.error):e(new Error(s.message,{cause:s})),s.preventDefault()},r=()=>{n.addEventListener("unhandledrejection",t),n.addEventListener("error",i)};typeof Zone<"u"?Zone.root.run(r):r(),$e(to).onDestroy(()=>{n.removeEventListener("error",i),n.removeEventListener("unhandledrejection",t)})}});function Hd(){return Pa([ym(()=>{$e(Zx)})])}var Wr=class{},no=new Ke("",{factory:()=>!0});var zd=new Ke("");var Gd=(()=>{class n{static \u0275prov=Et({token:n,providedIn:"root",factory:()=>new ed})}return n})(),ed=class{dirtyEffectCount=0;queues=new Map;add(e){this.enqueue(e),this.schedule(e)}schedule(e){e.dirty&&this.dirtyEffectCount++}remove(e){let t=e.zone,i=this.queues.get(t);i.has(e)&&(i.delete(e),e.dirty&&this.dirtyEffectCount--)}enqueue(e){let t=e.zone;this.queues.has(t)||this.queues.set(t,new Set);let i=this.queues.get(t);i.has(e)||i.add(e)}flush(){for(;this.dirtyEffectCount>0;){let e=!1;for(let[t,i]of this.queues)t===null?e||=this.flushQueue(i):e||=t.run(()=>this.flushQueue(i));e||(this.dirtyEffectCount=0)}}flushQueue(e){let t=!1;for(let i of e)i.dirty&&(this.dirtyEffectCount--,t=!0,i.run());return t}},td=class{[bi];constructor(e){this[bi]=e}destroy(){this[bi].destroy()}};function bg(n){return{toString:n}.toString()}function f0(n){return typeof n=="function"}function Tg(n,e,t,i){e!==null?e.applyValueToInputSignal(e,i):n[t]=i}var Ja=class{previousValue;currentValue;firstChange;constructor(e,t,i){this.previousValue=e,this.currentValue=t,this.firstChange=i}isFirstChange(){return this.firstChange}};function h0(n){return n.type.prototype.ngOnChanges&&(n.setInput=m0),p0}function p0(){let n=Ig(this),e=n?.current;if(e){let t=n.previous;if(t===sr)n.previous=e;else for(let i in e)t[i]=e[i];n.current=null,this.ngOnChanges(e)}}function m0(n,e,t,i,r){let s=this.declaredInputs[i],o=Ig(n)||g0(n,{previous:sr,current:null}),a=o.current||(o.current={}),c=o.previous,l=c[s];a[s]=new Ja(l&&l.currentValue,t,c===sr),Tg(n,e,r,t)}var wg="__ngSimpleChanges__";function Ig(n){return n[wg]||null}function g0(n,e){return n[wg]=e}var eg=[];var ft=function(n,e=null,t){for(let i=0;i<eg.length;i++){let r=eg[i];r(n,e,t)}},et=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(et||{});function v0(n,e,t){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:s}=e.type.prototype;if(i){let o=h0(e);(t.preOrderHooks??=[]).push(n,o),(t.preOrderCheckHooks??=[]).push(n,o)}r&&(t.preOrderHooks??=[]).push(0-n,r),s&&((t.preOrderHooks??=[]).push(n,s),(t.preOrderCheckHooks??=[]).push(n,s))}function y0(n,e){for(let t=e.directiveStart,i=e.directiveEnd;t<i;t++){let s=n.data[t].type.prototype,{ngAfterContentInit:o,ngAfterContentChecked:a,ngAfterViewInit:c,ngAfterViewChecked:l,ngOnDestroy:u}=s;o&&(n.contentHooks??=[]).push(-t,o),a&&((n.contentHooks??=[]).push(t,a),(n.contentCheckHooks??=[]).push(t,a)),c&&(n.viewHooks??=[]).push(-t,c),l&&((n.viewHooks??=[]).push(t,l),(n.viewCheckHooks??=[]).push(t,l)),u!=null&&(n.destroyHooks??=[]).push(t,u)}}function qa(n,e,t){Cg(n,e,3,t)}function Xa(n,e,t,i){(n[Le]&3)===t&&Cg(n,e,t,i)}function Wd(n,e){let t=n[Le];(t&3)===e&&(t&=16383,t+=1,n[Le]=t)}function Cg(n,e,t,i){let r=i!==void 0?n[ar]&65535:0,s=i??-1,o=e.length-1,a=0;for(let c=r;c<o;c++)if(typeof e[c+1]=="number"){if(a=e[c],i!=null&&a>=i)break}else e[c]<0&&(n[ar]+=65536),(a<s||s==-1)&&(_0(n,t,e,c),n[ar]=(n[ar]&4294901760)+c+2),c++}function tg(n,e){ft(et.LifecycleHookStart,n,e);let t=je(null);try{e.call(n)}finally{je(t),ft(et.LifecycleHookEnd,n,e)}}function _0(n,e,t,i){let r=t[i]<0,s=t[i+1],o=r?-t[i]:t[i],a=n[o];r?n[Le]>>14<n[ar]>>16&&(n[Le]&3)===e&&(n[Le]+=16384,tg(a,s)):tg(a,s)}var rs=-1,oo=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(e,t,i,r){this.factory=e,this.name=r,this.canSeeViewProviders=t,this.injectImpl=i}};function x0(n){return(n.flags&8)!==0}function M0(n){return(n.flags&16)!==0}function S0(n,e,t){let i=0;for(;i<t.length;){let r=t[i];if(typeof r=="number"){if(r!==0)break;i++;let s=t[i++],o=t[i++],a=t[i++];n.setAttribute(e,o,a,s)}else{let s=r,o=t[++i];b0(s)?n.setProperty(e,s,o):n.setAttribute(e,s,o),i++}}return i}function E0(n){return n===3||n===4||n===6}function b0(n){return n.charCodeAt(0)===64}function ac(n,e){if(!(e===null||e.length===0))if(n===null||n.length===0)n=e.slice();else{let t=-1;for(let i=0;i<e.length;i++){let r=e[i];typeof r=="number"?t=r:t===0||(t===-1||t===2?ng(n,t,r,null,e[++i]):ng(n,t,r,null,null))}}return n}function ng(n,e,t,i,r){let s=0,o=n.length;if(e===-1)o=-1;else for(;s<n.length;){let a=n[s++];if(typeof a=="number"){if(a===e){o=-1;break}else if(a>e){o=s-1;break}}}for(;s<n.length;){let a=n[s];if(typeof a=="number")break;if(a===t){r!==null&&(n[s+1]=r);return}s++,r!==null&&s++}o!==-1&&(n.splice(o,0,e),s=o+1),n.splice(s++,0,t),r!==null&&n.splice(s++,0,r)}function Ag(n){return n!==rs}function Ka(n){return n&32767}function T0(n){return n>>16}function Qa(n,e){let t=T0(n),i=e;for(;t>0;)i=i[Yr],t--;return i}var Zd=!0;function ig(n){let e=Zd;return Zd=n,e}var w0=256,Dg=w0-1,Rg=5,I0=0,jn={};function C0(n,e,t){let i;typeof t=="string"?i=t.charCodeAt(0)||0:t.hasOwnProperty(ir)&&(i=t[ir]),i==null&&(i=t[ir]=I0++);let r=i&Dg,s=1<<r;e.data[n+(r>>Rg)]|=s}function Ng(n,e){let t=Pg(n,e);if(t!==-1)return t;let i=e[Re];i.firstCreatePass&&(n.injectorIndex=e.length,jd(i.data,n),jd(e,null),jd(i.blueprint,null));let r=Sf(n,e),s=n.injectorIndex;if(Ag(r)){let o=Ka(r),a=Qa(r,e),c=a[Re].data;for(let l=0;l<8;l++)e[s+l]=a[o+l]|c[o+l]}return e[s+8]=r,s}function jd(n,e){n.push(0,0,0,0,0,0,0,0,e)}function Pg(n,e){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||e[n.injectorIndex+8]===null?-1:n.injectorIndex}function Sf(n,e){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let t=0,i=null,r=e;for(;r!==null;){if(i=Ug(r),i===null)return rs;if(t++,r=r[Yr],i.injectorIndex!==-1)return i.injectorIndex|t<<16}return rs}function A0(n,e,t){C0(n,e,t)}function Lg(n,e,t){if(t&8||n!==void 0)return n;Aa(e,"NodeInjector")}function Og(n,e,t,i){if(t&8&&i===void 0&&(i=null),(t&3)===0){let r=n[or],s=nn(void 0);try{return r?r.get(e,i,t&8):dd(e,i,t&8)}finally{nn(s)}}return Lg(i,e,t)}function Fg(n,e,t,i=0,r){if(n!==null){if(e[Le]&2048&&!(i&2)){let o=P0(n,e,t,i,jn);if(o!==jn)return o}let s=kg(n,e,t,i,jn);if(s!==jn)return s}return Og(e,t,i,r)}function kg(n,e,t,i,r){let s=R0(t);if(typeof s=="function"){if(!Od(e,n,i))return i&1?Lg(r,t,i):Og(e,t,i,r);try{let o;if(o=s(i),o==null&&!(i&8))Aa(t);else return o}finally{Fd()}}else if(typeof s=="number"){let o=null,a=Pg(n,e),c=rs,l=i&1?e[on][Wt]:null;for((a===-1||i&4)&&(c=a===-1?Sf(n,e):e[a+8],c===rs||!sg(i,!1)?a=-1:(o=e[Re],a=Ka(c),e=Qa(c,e)));a!==-1;){let u=e[Re];if(rg(s,a,u.data)){let f=D0(a,e,t,o,i,l);if(f!==jn)return f}c=e[a+8],c!==rs&&sg(i,e[Re].data[a+8]===l)&&rg(s,a,e)?(o=u,a=Ka(c),e=Qa(c,e)):a=-1}}return r}function D0(n,e,t,i,r,s){let o=e[Re],a=o.data[n+8],c=i==null?Jr(a)&&Zd:i!=o&&(a.type&3)!==0,l=r&1&&s===a,u=Ya(a,o,t,c,l);return u!==null?ec(e,o,u,a,r):jn}function Ya(n,e,t,i,r){let s=n.providerIndexes,o=e.data,a=s&1048575,c=n.directiveStart,l=n.directiveEnd,u=s>>20,f=i?a:a+u,d=r?a+u:l;for(let h=f;h<d;h++){let v=o[h];if(h<c&&t===v||h>=c&&v.type===t)return h}if(r){let h=o[c];if(h&&lr(h)&&h.type===t)return c}return null}function ec(n,e,t,i,r){let s=n[t],o=e.data;if(s instanceof oo){let a=s;if(a.resolving)throw ud("");let c=ig(a.canSeeViewProviders);a.resolving=!0;let l=o[t].type||o[t],u,f=a.injectImpl?nn(a.injectImpl):null,d=Od(n,i,0);try{s=n[t]=a.factory(void 0,r,o,n,i),e.firstCreatePass&&t>=i.directiveStart&&v0(t,o[t],e)}finally{f!==null&&nn(f),ig(c),a.resolving=!1,Fd()}}return s}function R0(n){if(typeof n=="string")return n.charCodeAt(0)||0;let e=n.hasOwnProperty(ir)?n[ir]:void 0;return typeof e=="number"?e>=0?e&Dg:N0:e}function rg(n,e,t){let i=1<<n;return!!(t[e+(n>>Rg)]&i)}function sg(n,e){return!(n&2)&&!(n&1&&e)}var hr=class{_tNode;_lView;constructor(e,t){this._tNode=e,this._lView=t}get(e,t,i){return Fg(this._tNode,this._lView,e,er(i),t)}};function N0(){return new hr(ci(),Ct())}function P0(n,e,t,i,r){let s=n,o=e;for(;s!==null&&o!==null&&o[Le]&2048&&!Kr(o);){let a=kg(s,o,t,i|2,jn);if(a!==jn)return a;let c=s.parent;if(!c){let l=o[yd];if(l){let u=l.get(t,jn,i&-5);if(u!==jn)return u}c=Ug(o),o=o[Yr]}s=c}return r}function Ug(n){let e=n[Re],t=e.type;return t===2?e.declTNode:t===1?n[Wt]:null}function L0(){return ls(ci(),Ct())}function ls(n,e){return new ho(Di(n,e))}var ho=(()=>{class n{nativeElement;constructor(t){this.nativeElement=t}static __NG_ELEMENT_ID__=L0}return n})();function O0(n){return n instanceof ho?n.nativeElement:n}function F0(){return this._results[Symbol.iterator]()}var tc=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new ri}constructor(e=!1){this._emitDistinctChangesOnly=e}get(e){return this._results[e]}map(e){return this._results.map(e)}filter(e){return this._results.filter(e)}find(e){return this._results.find(e)}reduce(e,t){return this._results.reduce(e,t)}forEach(e){this._results.forEach(e)}some(e){return this._results.some(e)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(e,t){this.dirty=!1;let i=mm(e);(this._changesDetected=!pm(this._results,i,t))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(e){this._onDirty=e}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=F0};function Bg(n){return(n.flags&128)===128}var Ef=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Ef||{}),Vg=new Map,k0=0;function U0(){return k0++}function B0(n){Vg.set(n[Ii],n)}function Jd(n){Vg.delete(n[Ii])}var og="__ngContext__";function ss(n,e){Ai(e)?(n[og]=e[Ii],B0(e)):n[og]=e}function Hg(n){return Gg(n[Xr])}function zg(n){return Gg(n[vn])}function Gg(n){for(;n!==null&&!In(n);)n=n[vn];return n}var V0;function bf(n){V0=n}var cc=new Ke("",{factory:()=>H0}),H0="ng";var lc=new Ke(""),gr=new Ke("",{providedIn:"platform",factory:()=>"unknown"});var uc=new Ke("",{factory:()=>$e(yn).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var Wg=!1,jg=new Ke("",{factory:()=>Wg});function dc(n){return(n.flags&32)===32}var z0=()=>null;function $g(n,e,t=!1){return z0(n,e,t)}function qg(n,e){let t=n.contentQueries;if(t!==null){let i=je(null);try{for(let r=0;r<t.length;r+=2){let s=t[r],o=t[r+1];if(o!==-1){let a=n.data[o];Va(s),a.contentQueries(2,e[o],o)}}}finally{je(i)}}}function Kd(n,e,t){Va(0);let i=je(null);try{e(n,t)}finally{je(i)}}function Xg(n,e,t){if(_d(e)){let i=je(null);try{let r=e.directiveStart,s=e.directiveEnd;for(let o=r;o<s;o++){let a=n.data[o];if(a.contentQueries){let c=t[o];a.contentQueries(1,c,o)}}}finally{je(i)}}}var Rn=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(Rn||{});var Qd=class{changingThisBreaksApplicationSecurity;constructor(e){this.changingThisBreaksApplicationSecurity=e}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${nd})`}};function Yg(n){return n instanceof Qd?n.changingThisBreaksApplicationSecurity:n}function G0(n,e){return n.createText(e)}function Zg(n,e,t){return n.createElement(e,t)}function nc(n,e,t,i,r){n.insertBefore(e,t,i,r)}function Jg(n,e,t){n.appendChild(e,t)}function ag(n,e,t,i,r){i!==null?nc(n,e,t,i,r):Jg(n,e,t)}function W0(n,e,t,i){n.removeChild(null,e,t,i)}function j0(n,e,t){n.setAttribute(e,"style",t)}function $0(n,e,t){t===""?n.removeAttribute(e,"class"):n.setAttribute(e,"class",t)}function Kg(n,e,t){let{mergedAttrs:i,classes:r,styles:s}=t;i!==null&&S0(n,e,i),r!==null&&$0(n,e,r),s!==null&&j0(n,e,s)}function q0(n,e,t){let i=n.length;for(;;){let r=n.indexOf(e,t);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let s=e.length;if(r+s===i||n.charCodeAt(r+s)<=32)return r}t=r+1}}var Qg="ng-template";function X0(n,e,t,i){let r=0;if(i){for(;r<e.length&&typeof e[r]=="string";r+=2)if(e[r]==="class"&&q0(e[r+1].toLowerCase(),t,0)!==-1)return!0}else if(Tf(n))return!1;if(r=e.indexOf(1,r),r>-1){let s;for(;++r<e.length&&typeof(s=e[r])=="string";)if(s.toLowerCase()===t)return!0}return!1}function Tf(n){return n.type===4&&n.value!==Qg}function Y0(n,e,t){let i=n.type===4&&!t?Qg:n.value;return e===i}function Z0(n,e,t){let i=4,r=n.attrs,s=r!==null?Q0(r):0,o=!1;for(let a=0;a<e.length;a++){let c=e[a];if(typeof c=="number"){if(!o&&!An(i)&&!An(c))return!1;if(o&&An(c))continue;o=!1,i=c|i&1;continue}if(!o)if(i&4){if(i=2|i&1,c!==""&&!Y0(n,c,t)||c===""&&e.length===1){if(An(i))return!1;o=!0}}else if(i&8){if(r===null||!X0(n,r,c,t)){if(An(i))return!1;o=!0}}else{let l=e[++a],u=J0(c,r,Tf(n),t);if(u===-1){if(An(i))return!1;o=!0;continue}if(l!==""){let f;if(u>s?f="":f=r[u+1].toLowerCase(),i&2&&l!==f){if(An(i))return!1;o=!0}}}}return An(i)||o}function An(n){return(n&1)===0}function J0(n,e,t,i){if(e===null)return-1;let r=0;if(i||!t){let s=!1;for(;r<e.length;){let o=e[r];if(o===n)return r;if(o===3||o===6)s=!0;else if(o===1||o===2){let a=e[++r];for(;typeof a=="string";)a=e[++r];continue}else{if(o===4)break;if(o===0){r+=4;continue}}r+=s?1:2}return-1}else return eM(e,n)}function ev(n,e,t=!1){for(let i=0;i<e.length;i++)if(Z0(n,e[i],t))return!0;return!1}function K0(n){let e=n.attrs;if(e!=null){let t=e.indexOf(5);if((t&1)===0)return e[t+1]}return null}function Q0(n){for(let e=0;e<n.length;e++){let t=n[e];if(E0(t))return e}return n.length}function eM(n,e){let t=n.indexOf(4);if(t>-1)for(t++;t<n.length;){let i=n[t];if(typeof i=="number")return-1;if(i===e)return t;t++}return-1}function tM(n,e){e:for(let t=0;t<e.length;t++){let i=e[t];if(n.length===i.length){for(let r=0;r<n.length;r++)if(n[r]!==i[r])continue e;return!0}}return!1}function cg(n,e){return n?":not("+e.trim()+")":e}function nM(n){let e=n[0],t=1,i=2,r="",s=!1;for(;t<n.length;){let o=n[t];if(typeof o=="string")if(i&2){let a=n[++t];r+="["+o+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+o:i&4&&(r+=" "+o);else r!==""&&!An(o)&&(e+=cg(s,r),r=""),i=o,s=s||!An(i);t++}return r!==""&&(e+=cg(s,r)),e}function iM(n){return n.map(nM).join(",")}function rM(n){let e=[],t=[],i=1,r=2;for(;i<n.length;){let s=n[i];if(typeof s=="string")r===2?s!==""&&e.push(s,n[++i]):r===8&&t.push(s);else{if(!An(r))break;r=s}i++}return t.length&&e.push(1,...t),e}var vr={};function wf(n,e,t,i,r,s,o,a,c,l,u){let f=Bt+i,d=f+r,h=sM(f,d),v=typeof l=="function"?l():l;return h[Re]={type:n,blueprint:h,template:t,queries:null,viewQuery:a,declTNode:e,data:h.slice().fill(null,f),bindingStartIndex:f,expandoStartIndex:d,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof s=="function"?s():s,pipeRegistry:typeof o=="function"?o():o,firstChild:null,schemas:c,consts:v,incompleteFirstPass:!1,ssrId:u}}function sM(n,e){let t=[];for(let i=0;i<e;i++)t.push(i<n?null:vr);return t}function oM(n){let e=n.tView;return e===null||e.incompleteFirstPass?n.tView=wf(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):e}function If(n,e,t,i,r,s,o,a,c,l,u){let f=e.blueprint.slice();return f[wn]=r,f[Le]=i|4|128|8|64|1024,(l!==null||n&&n[Le]&2048)&&(f[Le]|=2048),Sd(f),f[Rt]=f[Yr]=n,f[sn]=t,f[Gn]=o||n&&n[Gn],f[Ut]=a||n&&n[Ut],f[or]=c||n&&n[or]||null,f[Wt]=s,f[Ii]=U0(),f[qr]=u,f[yd]=l,f[on]=e.type==2?n[on]:f,f}function aM(n,e,t){let i=Di(e,n),r=oM(t),s=n[Gn].rendererFactory,o=Cf(n,If(n,r,null,tv(t),i,e,null,s.createRenderer(i,t),null,null,null));return n[e.index]=o}function tv(n){let e=16;return n.signals?e=4096:n.onPush&&(e=64),e}function nv(n,e,t,i){if(t===0)return-1;let r=e.length;for(let s=0;s<t;s++)e.push(i),n.blueprint.push(i),n.data.push(null);return r}function Cf(n,e){return n[Xr]?n[vd][vn]=e:n[Xr]=e,n[vd]=e,e}function cM(n,e,t,i){if(!i)if((e[Le]&3)===3){let s=n.preOrderCheckHooks;s!==null&&qa(e,s,t)}else{let s=n.preOrderHooks;s!==null&&Xa(e,s,0,t)}Ni(t)}var fc=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(fc||{});function ef(n,e,t,i){let r=je(null);try{let[s,o,a]=n.inputs[t],c=null;(o&fc.SignalBased)!==0&&(c=e[s][bi]),c!==null&&c.transformFn!==void 0?i=c.transformFn(i):a!==null&&(i=a.call(e,i)),n.setInput!==null?n.setInput(e,c,i,t,s):Tg(e,c,s,i)}finally{je(r)}}var li=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(li||{}),lM;function Af(n,e){return lM(n,e)}var R1=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var tf=new WeakMap,io=new WeakSet;function uM(n,e){let t=tf.get(n);if(!t||t.length===0)return;let i=e.parentNode,r=e.previousSibling;for(let s=t.length-1;s>=0;s--){let o=t[s],a=o.parentNode;o===e?(t.splice(s,1),io.add(o),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&o===r||a&&i&&a!==i)&&(t.splice(s,1),o.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),o.parentNode?.removeChild(o))}}function dM(n,e){let t=tf.get(n);t?t.includes(e)||t.push(e):tf.set(n,[e])}var os=new Set,Df=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(Df||{}),us=new Ke(""),lg=new Set;function iv(n){lg.has(n)||(lg.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var rv=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=Et({token:n,providedIn:"root",factory:()=>new n})}return n})();var fM=new Ke("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:$e(gn)})});function sv(n,e,t){let i=n.get(fM);if(Array.isArray(e))for(let r of e)i.queue.add(r),t?.detachedLeaveAnimationFns?.push(r);else i.queue.add(e),t?.detachedLeaveAnimationFns?.push(e);i.scheduler&&i.scheduler(n)}function hM(n,e){for(let[t,i]of e)sv(n,i.animateFns)}function ug(n,e,t,i){let r=n?.[Zr]?.enter;e!==null&&r&&r.has(t.index)&&hM(i,r)}function is(n,e,t,i,r,s,o,a){if(r!=null){let c,l=!1;In(r)?c=r:Ai(r)&&(l=!0,r=r[wn]);let u=Cn(r);n===0&&i!==null?(ug(a,i,s,t),o==null?Jg(e,i,u):nc(e,i,u,o||null,!0)):n===1&&i!==null?(ug(a,i,s,t),nc(e,i,u,o||null,!0),uM(s,u)):n===2?(a?.[Zr]?.leave?.has(s.index)&&dM(s,u),io.delete(u),dg(a,s,t,f=>{if(io.has(u)){io.delete(u);return}W0(e,u,l,f)})):n===3&&(io.delete(u),dg(a,s,t,()=>{e.destroyNode(u)})),c!=null&&bM(e,n,t,c,s,i,o)}}function pM(n,e){ov(n,e),e[wn]=null,e[Wt]=null}function mM(n,e,t,i,r,s){i[wn]=r,i[Wt]=e,hc(n,i,t,1,r,s)}function ov(n,e){e[Gn].changeDetectionScheduler?.notify(9),hc(n,e,e[Ut],2,null,null)}function gM(n){let e=n[Xr];if(!e)return $d(n[Re],n);for(;e;){let t=null;if(Ai(e))t=e[Xr];else{let i=e[jt];i&&(t=i)}if(!t){for(;e&&!e[vn]&&e!==n;)Ai(e)&&$d(e[Re],e),e=e[Rt];e===null&&(e=n),Ai(e)&&$d(e[Re],e),t=e&&e[vn]}e=t}}function Rf(n,e){let t=n[cr],i=t.indexOf(e);t.splice(i,1)}function av(n,e){if(ur(e))return;let t=e[Ut];t.destroyNode&&hc(n,e,t,3,null,null),gM(e)}function $d(n,e){if(ur(e))return;let t=je(null);try{e[Le]&=-129,e[Le]|=256,e[an]&&Bs(e[an]),_M(n,e),yM(n,e),e[Re].type===1&&e[Ut].destroy();let i=e[wi];if(i!==null&&In(e[Rt])){i!==e[Rt]&&Rf(i,e);let r=e[Wn];r!==null&&r.detachView(n)}Jd(e)}finally{je(t)}}function dg(n,e,t,i){let r=n?.[Zr];if(r==null||r.leave==null||!r.leave.has(e.index))return i(!1);n&&os.add(n[Ii]),sv(t,()=>{if(r.leave&&r.leave.has(e.index)){let o=r.leave.get(e.index),a=[];if(o){for(let c=0;c<o.animateFns.length;c++){let l=o.animateFns[c],{promise:u}=l();a.push(u)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),vM(n,i)}else n&&os.delete(n[Ii]),i(!1)},r)}function vM(n,e){let t=n[Zr]?.running;if(t){t.then(()=>{n[Zr].running=void 0,os.delete(n[Ii]),e(!0)});return}e(!1)}function yM(n,e){let t=n.cleanup,i=e[Ys];if(t!==null)for(let o=0;o<t.length-1;o+=2)if(typeof t[o]=="string"){let a=t[o+3];a>=0?i[a]():i[-a].unsubscribe(),o+=2}else{let a=i[t[o+1]];t[o].call(a)}i!==null&&(e[Ys]=null);let r=e[oi];if(r!==null){e[oi]=null;for(let o=0;o<r.length;o++){let a=r[o];a()}}let s=e[Zs];if(s!==null){e[Zs]=null;for(let o of s)o.destroy()}}function _M(n,e){let t;if(n!=null&&(t=n.destroyHooks)!=null)for(let i=0;i<t.length;i+=2){let r=e[t[i]];if(!(r instanceof oo)){let s=t[i+1];if(Array.isArray(s))for(let o=0;o<s.length;o+=2){let a=r[s[o]],c=s[o+1];ft(et.LifecycleHookStart,a,c);try{c.call(a)}finally{ft(et.LifecycleHookEnd,a,c)}}else{ft(et.LifecycleHookStart,r,s);try{s.call(r)}finally{ft(et.LifecycleHookEnd,r,s)}}}}}function cv(n,e,t){return xM(n,e.parent,t)}function xM(n,e,t){let i=e;for(;i!==null&&i.type&168;)e=i,i=e.parent;if(i===null)return t[wn];if(Jr(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===Rn.None||r===Rn.Emulated)return null}return Di(i,t)}function lv(n,e,t){return SM(n,e,t)}function MM(n,e,t){return n.type&40?Di(n,t):null}var SM=MM,fg;function Nf(n,e,t,i){let r=cv(n,i,e),s=e[Ut],o=i.parent||e[Wt],a=lv(o,i,e);if(r!=null)if(Array.isArray(t))for(let c=0;c<t.length;c++)ag(s,r,t[c],a,!1);else ag(s,r,t,a,!1);fg!==void 0&&fg(s,i,e,t,r)}function ro(n,e){if(e!==null){let t=e.type;if(t&3)return Di(e,n);if(t&4)return nf(-1,n[e.index]);if(t&8){let i=e.child;if(i!==null)return ro(n,i);{let r=n[e.index];return In(r)?nf(-1,r):Cn(r)}}else{if(t&128)return ro(n,e.next);if(t&32)return Af(e,n)()||Cn(n[e.index]);{let i=uv(n,e);if(i!==null){if(Array.isArray(i))return i[0];let r=Ti(n[on]);return ro(r,i)}else return ro(n,e.next)}}}return null}function uv(n,e){if(e!==null){let i=n[on][Wt],r=e.projection;return i.projection[r]}return null}function nf(n,e){let t=jt+n+1;if(t<e.length){let i=e[t],r=i[Re].firstChild;if(r!==null)return ro(i,r)}return e[Ci]}function Pf(n,e,t,i,r,s,o){for(;t!=null;){let a=i[or];if(t.type===128){t=t.next;continue}let c=i[t.index],l=t.type;if(o&&e===0&&(c&&ss(Cn(c),i),t.flags|=2),!dc(t))if(l&8)Pf(n,e,t.child,i,r,s,!1),is(e,n,a,r,c,t,s,i);else if(l&32){let u=Af(t,i),f;for(;f=u();)is(e,n,a,r,f,t,s,i);is(e,n,a,r,c,t,s,i)}else l&16?dv(n,e,i,t,r,s):is(e,n,a,r,c,t,s,i);t=o?t.projectionNext:t.next}}function hc(n,e,t,i,r,s){Pf(t,i,n.firstChild,e,r,s,!1)}function EM(n,e,t){let i=e[Ut],r=cv(n,t,e),s=t.parent||e[Wt],o=lv(s,t,e);dv(i,0,e,t,r,o)}function dv(n,e,t,i,r,s){let o=t[on],c=o[Wt].projection[i.projection];if(Array.isArray(c))for(let l=0;l<c.length;l++){let u=c[l];is(e,n,t[or],r,u,i,s,t)}else{let l=c,u=o[Rt];Bg(i)&&(l.flags|=128),Pf(n,e,l,u,r,s,!0)}}function bM(n,e,t,i,r,s,o){let a=i[Ci],c=Cn(i);a!==c&&is(e,n,t,s,a,r,o);for(let l=jt;l<i.length;l++){let u=i[l];hc(u[Re],u,n,e,s,a)}}function TM(n,e,t,i,r){if(e)r?n.addClass(t,i):n.removeClass(t,i);else{let s=i.indexOf("-")===-1?void 0:li.DashCase;r==null?n.removeStyle(t,i,s):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),s|=li.Important),n.setStyle(t,i,r,s))}}function fv(n,e,t,i,r){let s=eo(),o=i&2;try{Ni(-1),o&&e.length>Bt&&cM(n,e,Bt,!1);let a=o?et.TemplateUpdateStart:et.TemplateCreateStart;ft(a,r,t),t(i,r)}finally{Ni(s);let a=o?et.TemplateUpdateEnd:et.TemplateCreateEnd;ft(a,r,t)}}function hv(n,e,t){AM(n,e,t),(t.flags&64)===64&&DM(n,e,t)}function Lf(n,e,t=Di){let i=e.localNames;if(i!==null){let r=e.index+1;for(let s=0;s<i.length;s+=2){let o=i[s+1],a=o===-1?t(e,n):n[o];n[r++]=a}}}function wM(n,e,t,i){let s=i.get(jg,Wg)||t===Rn.ShadowDom||t===Rn.ExperimentalIsolatedShadowDom,o=n.selectRootElement(e,s);if(o.tagName.toLowerCase()==="script")throw new Ge(905,!1);return IM(o),o}function IM(n){CM(n)}var CM=()=>null;function AM(n,e,t){let i=t.directiveStart,r=t.directiveEnd;Jr(t)&&aM(e,t,n.data[i+t.componentOffset]),n.firstCreatePass||Ng(t,e);let s=t.initialInputs;for(let o=i;o<r;o++){let a=n.data[o],c=ec(e,n,o,t);if(ss(c,e),s!==null&&PM(e,o-i,c,a,t,s),lr(a)){let l=Ri(t.index,e);l[sn]=ec(e,n,o,t)}}}function DM(n,e,t){let i=t.directiveStart,r=t.directiveEnd,s=t.index,o=zm();try{Ni(s);for(let a=i;a<r;a++){let c=n.data[a],l=e[a];Ba(a),(c.hostBindings!==null||c.hostVars!==0||c.hostAttrs!==null)&&RM(c,l)}}finally{Ni(-1),Ba(o)}}function RM(n,e){n.hostBindings!==null&&n.hostBindings(1,e)}function NM(n,e){let t=n.directiveRegistry,i=null;if(t)for(let r=0;r<t.length;r++){let s=t[r];ev(e,s.selectors,!1)&&(i??=[],lr(s)?i.unshift(s):i.push(s))}return i}function PM(n,e,t,i,r,s){let o=s[e];if(o!==null)for(let a=0;a<o.length;a+=2){let c=o[a],l=o[a+1];ef(i,t,c,l)}}function pv(n,e,t,i,r){let s=Bt+t,o=e[Re],a=r(o,e,n,i,t);e[s]=a,es(n,!0);let c=n.type===2;return c?(Kg(e[Ut],a,n),(Lm()===0||xd(n))&&ss(a,e),Om()):ss(a,e),Wa()&&(!c||!dc(n))&&Nf(o,e,a,n),n}function mv(n){let e=n;return Dd()?Rd():(e=e.parent,es(e,!1)),e}function gv(n,e,t,i,r){let s=n.inputs?.[i],o=n.hostDirectiveInputs?.[i],a=!1;if(o)for(let c=0;c<o.length;c+=2){let l=o[c],u=o[c+1],f=e.data[l];ef(f,t[l],u,r),a=!0}if(s)for(let c of s){let l=t[c],u=e.data[c];ef(u,l,i,r),a=!0}return a}function LM(n,e){let t=Ri(e,n),i=t[Re];OM(i,t);let r=t[wn];r!==null&&t[qr]===null&&(t[qr]=$g(r,t[or])),ft(et.ComponentStart);try{Of(i,t,t[sn])}finally{ft(et.ComponentEnd,t[sn])}}function OM(n,e){for(let t=e.length;t<n.blueprint.length;t++)e.push(n.blueprint[t])}function Of(n,e,t){Ha(e);try{let i=n.viewQuery;i!==null&&Kd(1,i,t);let r=n.template;r!==null&&fv(n,e,r,1,t),n.firstCreatePass&&(n.firstCreatePass=!1),e[Wn]?.finishViewCreation(n),n.staticContentQueries&&qg(n,e),n.staticViewQueries&&Kd(2,n.viewQuery,t);let s=n.components;s!==null&&FM(e,s)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{e[Le]&=-5,za()}}function FM(n,e){for(let t=0;t<e.length;t++)LM(n,e[t])}function vv(n,e,t,i){let r=je(null);try{let s=e.tView,a=n[Le]&4096?4096:16,c=If(n,s,t,a,null,e,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),l=n[e.index];c[wi]=l;let u=n[Wn];return u!==null&&(c[Wn]=u.createEmbeddedView(s)),Of(s,c,t),c}finally{je(r)}}function rf(n,e){return!e||e.firstChild===null||Bg(n)}function ao(n,e,t,i,r=!1){for(;t!==null;){if(t.type===128){t=r?t.projectionNext:t.next;continue}let s=e[t.index];s!==null&&i.push(Cn(s)),In(s)&&yv(s,i);let o=t.type;if(o&8)ao(n,e,t.child,i);else if(o&32){let a=Af(t,e),c;for(;c=a();)i.push(c)}else if(o&16){let a=uv(e,t);if(Array.isArray(a))i.push(...a);else{let c=Ti(e[on]);ao(c[Re],c,a,i,!0)}}t=r?t.projectionNext:t.next}return i}function yv(n,e){for(let t=jt;t<n.length;t++){let i=n[t],r=i[Re].firstChild;r!==null&&ao(i[Re],i,r,e)}n[Ci]!==n[wn]&&e.push(n[Ci])}function _v(n){if(n[Fa]!==null){for(let e of n[Fa])e.impl.addSequence(e);n[Fa].length=0}}var xv=[];function kM(n){return n[an]??UM(n)}function UM(n){let e=xv.pop()??Object.create(VM);return e.lView=n,e}function BM(n){n.lView[an]!==n&&(n.lView=null,xv.push(n))}var VM=Xt(Lt({},da),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Qs(n.lView)},consumerOnSignalRead(){this.lView[an]=this}});function HM(n){let e=n[an]??Object.create(zM);return e.lView=n,e}var zM=Xt(Lt({},da),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let e=Ti(n.lView);for(;e&&!Mv(e[Re]);)e=Ti(e);e&&Ed(e)},consumerOnSignalRead(){this.lView[an]=this}});function Mv(n){return n.type!==2}function Sv(n){if(n[Zs]===null)return;let e=!0;for(;e;){let t=!1;for(let i of n[Zs])i.dirty&&(t=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));e=t&&!!(n[Le]&8192)}}var GM=100;function Ev(n,e=0){let i=n[Gn].rendererFactory,r=!1;r||i.begin?.();try{WM(n,e)}finally{r||i.end?.()}}function WM(n,e){let t=Nd();try{Pd(!0),sf(n,e);let i=0;for(;Ks(n);){if(i===GM)throw new Ge(103,!1);i++,sf(n,1)}}finally{Pd(t)}}function jM(n,e,t,i){if(ur(e))return;let r=e[Le],s=!1,o=!1;Ha(e);let a=!0,c=null,l=null;s||(Mv(n)?(l=kM(e),c=fa(l)):ua()===null?(a=!1,l=HM(e),c=fa(l)):e[an]&&(Bs(e[an]),e[an]=null));try{Sd(e),Um(n.bindingStartIndex),t!==null&&fv(n,e,t,2,i);let u=(r&3)===3;if(!s)if(u){let h=n.preOrderCheckHooks;h!==null&&qa(e,h,null)}else{let h=n.preOrderHooks;h!==null&&Xa(e,h,0,null),Wd(e,0)}if(o||$M(e),Sv(e),bv(e,0),n.contentQueries!==null&&qg(n,e),!s)if(u){let h=n.contentCheckHooks;h!==null&&qa(e,h)}else{let h=n.contentHooks;h!==null&&Xa(e,h,1),Wd(e,1)}XM(n,e);let f=n.components;f!==null&&wv(e,f,0);let d=n.viewQuery;if(d!==null&&Kd(2,d,i),!s)if(u){let h=n.viewCheckHooks;h!==null&&qa(e,h)}else{let h=n.viewHooks;h!==null&&Xa(e,h,2),Wd(e,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),e[Oa]){for(let h of e[Oa])h();e[Oa]=null}s||(_v(e),e[Le]&=-73)}catch(u){throw s||Qs(e),u}finally{l!==null&&(Iu(l,c),a&&BM(l)),za()}}function bv(n,e){for(let t=Hg(n);t!==null;t=zg(t))for(let i=jt;i<t.length;i++){let r=t[i];Tv(r,e)}}function $M(n){for(let e=Hg(n);e!==null;e=zg(e)){if(!(e[Le]&2))continue;let t=e[cr];for(let i=0;i<t.length;i++){let r=t[i];Ed(r)}}}function qM(n,e,t){ft(et.ComponentStart);let i=Ri(e,n);try{Tv(i,t)}finally{ft(et.ComponentEnd,i[sn])}}function Tv(n,e){ka(n)&&sf(n,e)}function sf(n,e){let i=n[Re],r=n[Le],s=n[an],o=!!(e===0&&r&16);if(o||=!!(r&64&&e===0),o||=!!(r&1024),o||=!!(s?.dirty&&ha(s)),o||=!1,s&&(s.dirty=!1),n[Le]&=-9217,o)jM(i,n,i.template,n[sn]);else if(r&8192){let a=je(null);try{Sv(n),bv(n,1);let c=i.components;c!==null&&wv(n,c,1),_v(n)}finally{je(a)}}}function wv(n,e,t){for(let i=0;i<e.length;i++)qM(n,e[i],t)}function XM(n,e){let t=n.hostBindingOpCodes;if(t!==null)try{for(let i=0;i<t.length;i++){let r=t[i];if(r<0)Ni(~r);else{let s=r,o=t[++i],a=t[++i];Hm(o,s);let c=e[s];ft(et.HostBindingsUpdateStart,c);try{a(2,c)}finally{ft(et.HostBindingsUpdateEnd,c)}}}}finally{Ni(-1)}}function Iv(n,e){let t=Nd()?64:1088;for(n[Gn].changeDetectionScheduler?.notify(e);n;){n[Le]|=t;let i=Ti(n);if(Kr(n)&&!i)return n;n=i}return null}function Cv(n,e,t,i){return[n,!0,0,e,null,i,null,t,null,null]}function Av(n,e,t,i=!0){let r=e[Re];if(YM(r,e,n,t),i){let o=nf(t,n),a=e[Ut],c=a.parentNode(n[Ci]);c!==null&&mM(r,n[Wt],a,e,c,o)}let s=e[qr];s!==null&&s.firstChild!==null&&(s.firstChild=null)}function of(n,e){if(n.length<=jt)return;let t=jt+e,i=n[t];if(i){let r=i[wi];r!==null&&r!==n&&Rf(r,i),e>0&&(n[t-1][vn]=i[vn]);let s=$s(n,jt+e);pM(i[Re],i);let o=s[Wn];o!==null&&o.detachView(s[Re]),i[Rt]=null,i[vn]=null,i[Le]&=-129}return i}function YM(n,e,t,i){let r=jt+i,s=t.length;i>0&&(t[r-1][vn]=e),i<s-jt?(e[vn]=t[r],fd(t,jt+i,e)):(t.push(e),e[vn]=null),e[Rt]=t;let o=e[wi];o!==null&&t!==o&&Dv(o,e);let a=e[Wn];a!==null&&a.insertView(n),Ua(e),e[Le]|=128}function Dv(n,e){let t=n[cr],i=e[Rt];if(Ai(i))n[Le]|=2;else{let r=i[Rt][on];e[on]!==r&&(n[Le]|=2)}t===null?n[cr]=[e]:t.push(e)}var as=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let e=this._lView,t=e[Re];return ao(t,e,t.firstChild,[])}constructor(e,t){this._lView=e,this._cdRefInjectingView=t}get context(){return this._lView[sn]}set context(e){this._lView[sn]=e}get destroyed(){return ur(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let e=this._lView[Rt];if(In(e)){let t=e[Js],i=t?t.indexOf(this):-1;i>-1&&(of(e,i),$s(t,i))}this._attachedToViewContainer=!1}av(this._lView[Re],this._lView)}onDestroy(e){bd(this._lView,e)}markForCheck(){Iv(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[Le]&=-129}reattach(){Ua(this._lView),this._lView[Le]|=128}detectChanges(){this._lView[Le]|=1024,Ev(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new Ge(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let e=Kr(this._lView),t=this._lView[wi];t!==null&&!e&&Rf(t,this._lView),ov(this._lView[Re],this._lView)}attachToAppRef(e){if(this._attachedToViewContainer)throw new Ge(902,!1);this._appRef=e;let t=Kr(this._lView),i=this._lView[wi];i!==null&&!t&&Dv(i,this._lView),Ua(this._lView)}};var co=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=ZM;constructor(t,i,r){this._declarationLView=t,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(t,i){return this.createEmbeddedViewImpl(t,i)}createEmbeddedViewImpl(t,i,r){let s=vv(this._declarationLView,this._declarationTContainer,t,{embeddedViewInjector:i,dehydratedView:r});return new as(s)}}return n})();function ZM(){return Ff(ci(),Ct())}function Ff(n,e){return n.type&4?new co(e,n,ls(n,e)):null}function po(n,e,t,i,r){let s=n.data[e];if(s===null)s=JM(n,e,t,i,r),Vm()&&(s.flags|=32);else if(s.type&64){s.type=t,s.value=i,s.attrs=r;let o=km();s.injectorIndex=o===null?-1:o.injectorIndex}return es(s,!0),s}function JM(n,e,t,i,r){let s=Ad(),o=Dd(),a=o?s:s&&s.parent,c=n.data[e]=QM(n,a,t,e,i,r);return KM(n,c,s,o),c}function KM(n,e,t,i){n.firstChild===null&&(n.firstChild=e),t!==null&&(i?t.child==null&&e.parent!==null&&(t.child=e):t.next===null&&(t.next=e,e.prev=t))}function QM(n,e,t,i,r,s){let o=e?e.injectorIndex:-1,a=0;return wd()&&(a|=128),{type:t,index:i,insertBeforeIndex:null,injectorIndex:o,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:kd(),attrs:s,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:e,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}var eS=()=>null;function af(n,e){return eS(n,e)}var Rv=class{},pc=class{},cf=class{resolveComponentFactory(e){throw new Ge(917,!1)}},mc=class{static NULL=new cf},pr=class{};var Nv=(()=>{class n{static \u0275prov=Et({token:n,providedIn:"root",factory:()=>null})}return n})();var Za={},lf=class{injector;parentInjector;constructor(e,t){this.injector=e,this.parentInjector=t}get(e,t,i){let r=this.injector.get(e,Za,i);return r!==Za||t===Za?r:this.parentInjector.get(e,t,i)}};function ic(n,e,t){let i=t?n.styles:null,r=t?n.classes:null,s=0;if(e!==null)for(let o=0;o<e.length;o++){let a=e[o];if(typeof a=="number")s=a;else if(s==1)r=Ta(r,a);else if(s==2){let c=a,l=e[++o];i=Ta(i,c+": "+l+";")}}t?n.styles=i:n.stylesWithoutHost=i,t?n.classes=r:n.classesWithoutHost=r}function Pv(n,e=0){let t=Ct();if(t===null)return at(n,e);let i=ci();return Fg(i,t,rn(n),e)}function tS(n,e,t,i,r){let s=i===null?null:{"":-1},o=r(n,t);if(o!==null){let a=o,c=null,l=null;for(let u of o)if(u.resolveHostDirectives!==null){[a,c,l]=u.resolveHostDirectives(o);break}rS(n,e,t,a,s,c,l)}s!==null&&i!==null&&nS(t,i,s)}function nS(n,e,t){let i=n.localNames=[];for(let r=0;r<e.length;r+=2){let s=t[e[r+1]];if(s==null)throw new Ge(-301,!1);i.push(e[r],s)}}function iS(n,e,t){e.componentOffset=t,(n.components??=[]).push(e.index)}function rS(n,e,t,i,r,s,o){let a=i.length,c=null;for(let d=0;d<a;d++){let h=i[d];c===null&&lr(h)&&(c=h,iS(n,t,d)),A0(Ng(t,e),n,h.type)}uS(t,n.data.length,a),c?.viewProvidersResolver&&c.viewProvidersResolver(c);for(let d=0;d<a;d++){let h=i[d];h.providersResolver&&h.providersResolver(h)}let l=!1,u=!1,f=nv(n,e,a,null);a>0&&(t.directiveToIndex=new Map);for(let d=0;d<a;d++){let h=i[d];if(t.mergedAttrs=ac(t.mergedAttrs,h.hostAttrs),oS(n,t,e,f,h),lS(f,h,r),o!==null&&o.has(h)){let[M,m]=o.get(h);t.directiveToIndex.set(h.type,[f,M+t.directiveStart,m+t.directiveStart])}else(s===null||!s.has(h))&&t.directiveToIndex.set(h.type,f);h.contentQueries!==null&&(t.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(t.flags|=64);let v=h.type.prototype;!l&&(v.ngOnChanges||v.ngOnInit||v.ngDoCheck)&&((n.preOrderHooks??=[]).push(t.index),l=!0),!u&&(v.ngOnChanges||v.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(t.index),u=!0),f++}sS(n,t,s)}function sS(n,e,t){for(let i=e.directiveStart;i<e.directiveEnd;i++){let r=n.data[i];if(t===null||!t.has(r))hg(0,e,r,i),hg(1,e,r,i),mg(e,i,!1);else{let s=t.get(r);pg(0,e,s,i),pg(1,e,s,i),mg(e,i,!0)}}}function hg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o;n===0?o=e.inputs??={}:o=e.outputs??={},o[s]??=[],o[s].push(i),Lv(e,s)}}function pg(n,e,t,i){let r=n===0?t.inputs:t.outputs;for(let s in r)if(r.hasOwnProperty(s)){let o=r[s],a;n===0?a=e.hostDirectiveInputs??={}:a=e.hostDirectiveOutputs??={},a[o]??=[],a[o].push(i,s),Lv(e,o)}}function Lv(n,e){e==="class"?n.flags|=8:e==="style"&&(n.flags|=16)}function mg(n,e,t){let{attrs:i,inputs:r,hostDirectiveInputs:s}=n;if(i===null||!t&&r===null||t&&s===null||Tf(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let o=null,a=0;for(;a<i.length;){let c=i[a];if(c===0){a+=4;continue}else if(c===5){a+=2;continue}else if(typeof c=="number")break;if(!t&&r.hasOwnProperty(c)){let l=r[c];for(let u of l)if(u===e){o??=[],o.push(c,i[a+1]);break}}else if(t&&s.hasOwnProperty(c)){let l=s[c];for(let u=0;u<l.length;u+=2)if(l[u]===e){o??=[],o.push(l[u+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(o)}function oS(n,e,t,i,r){n.data[i]=r;let s=r.factory||(r.factory=Gr(r.type,!0)),o=new oo(s,lr(r),Pv,null);n.blueprint[i]=o,t[i]=o,aS(n,e,i,nv(n,t,r.hostVars,vr),r)}function aS(n,e,t,i,r){let s=r.hostBindings;if(s){let o=n.hostBindingOpCodes;o===null&&(o=n.hostBindingOpCodes=[]);let a=~e.index;cS(o)!=a&&o.push(a),o.push(t,i,s)}}function cS(n){let e=n.length;for(;e>0;){let t=n[--e];if(typeof t=="number"&&t<0)return t}return 0}function lS(n,e,t){if(t){if(e.exportAs)for(let i=0;i<e.exportAs.length;i++)t[e.exportAs[i]]=n;lr(e)&&(t[""]=n)}}function uS(n,e,t){n.flags|=1,n.directiveStart=e,n.directiveEnd=e+t,n.providerIndexes=e}function Ov(n,e,t,i,r,s,o,a){let c=e[Re],l=c.consts,u=Qr(l,o),f=po(c,n,t,i,u);return s&&tS(c,e,f,Qr(l,a),r),f.mergedAttrs=ac(f.mergedAttrs,f.attrs),f.attrs!==null&&ic(f,f.attrs,!1),f.mergedAttrs!==null&&ic(f,f.mergedAttrs,!0),c.queries!==null&&c.queries.elementStart(c,f),f}function Fv(n,e){y0(n,e),_d(e)&&n.queries.elementEnd(e)}function dS(n,e,t,i,r,s){let o=e.consts,a=Qr(o,r),c=po(e,n,t,i,a);if(c.mergedAttrs=ac(c.mergedAttrs,c.attrs),s!=null){let l=Qr(o,s);c.localNames=[];for(let u=0;u<l.length;u+=2)c.localNames.push(l[u],-1)}return c.attrs!==null&&ic(c,c.attrs,!1),c.mergedAttrs!==null&&ic(c,c.mergedAttrs,!0),e.queries!==null&&e.queries.elementStart(e,c),c}function fS(n,e,t){if(t===vr)return!1;let i=n[e];return Object.is(i,t)?!1:(n[e]=t,!0)}var uf=Symbol("BINDING");function kv(n){return n.debugInfo?.className||n.type.name||null}var df=class extends mc{ngModule;constructor(e){super(),this.ngModule=e}resolveComponentFactory(e){let t=rr(e);return new lo(t,this.ngModule)}};function hS(n){return Object.keys(n).map(e=>{let[t,i,r]=n[e],s={propName:t,templateName:e,isSignal:(i&fc.SignalBased)!==0};return r&&(s.transform=r),s})}function pS(n){return Object.keys(n).map(e=>({propName:n[e],templateName:e}))}function mS(n,e,t){let i=e instanceof gn?e:e?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new lf(t,i):t}function gS(n){let e=n.get(pr,null);if(e===null)throw new Ge(407,!1);let t=n.get(Nv,null),i=n.get(Wr,null),r=n.get(us,null,{optional:!0});return{rendererFactory:e,sanitizer:t,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function vS(n,e){let t=Uv(n);return Zg(e,t,t==="svg"?Md:t==="math"?Tm:null)}function Uv(n){return(n.selectors[0][0]||"div").toLowerCase()}var lo=class extends pc{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=hS(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=pS(this.componentDef.outputs),this.cachedOutputs}constructor(e,t){super(),this.componentDef=e,this.ngModule=t,this.componentType=e.type,this.selector=iM(e.selectors),this.ngContentSelectors=e.ngContentSelectors??[],this.isBoundToModule=!!t}create(e,t,i,r,s,o){ft(et.DynamicComponentStart);let a=je(null);try{let c=this.componentDef,l=mS(c,r||this.ngModule,e),u=gS(l),f=u.tracingService;return f&&f.componentCreate?f.componentCreate(kv(c),()=>this.createComponentRef(u,l,t,i,s,o)):this.createComponentRef(u,l,t,i,s,o)}finally{je(a)}}createComponentRef(e,t,i,r,s,o){let a=this.componentDef,c=yS(r,a,o,s),l=e.rendererFactory.createRenderer(null,a),u=r?wM(l,r,a.encapsulation,t):vS(a,l),f=o?.some(gg)||s?.some(v=>typeof v!="function"&&v.bindings.some(gg)),d=If(null,c,null,512|tv(a),null,null,e,l,t,null,$g(u,t,!0));d[Bt]=u,Ha(d);let h=null;try{let v=Ov(Bt,d,2,"#host",()=>c.directiveRegistry,!0,0);Kg(l,u,v),ss(u,d),hv(c,d,v),Xg(c,v,d),Fv(c,v),i!==void 0&&xS(v,this.ngContentSelectors,i),h=Ri(v.index,d),d[sn]=h[sn],Of(c,d,null)}catch(v){throw h!==null&&Jd(h),Jd(d),v}finally{ft(et.DynamicComponentEnd),za()}return new rc(this.componentType,d,!!f)}};function yS(n,e,t,i){let r=n?["ng-version","21.2.14"]:rM(e.selectors[0]),s=null,o=null,a=0;if(t)for(let u of t)a+=u[uf].requiredVars,u.create&&(u.targetIdx=0,(s??=[]).push(u)),u.update&&(u.targetIdx=0,(o??=[]).push(u));if(i)for(let u=0;u<i.length;u++){let f=i[u];if(typeof f!="function")for(let d of f.bindings){a+=d[uf].requiredVars;let h=u+1;d.create&&(d.targetIdx=h,(s??=[]).push(d)),d.update&&(d.targetIdx=h,(o??=[]).push(d))}}let c=[e];if(i)for(let u of i){let f=typeof u=="function"?u:u.type,d=cd(f);c.push(d)}return wf(0,null,_S(s,o),1,a,c,null,null,null,[r],null)}function _S(n,e){return!n&&!e?null:t=>{if(t&1&&n)for(let i of n)i.create();if(t&2&&e)for(let i of e)i.update()}}function gg(n){let e=n[uf].kind;return e==="input"||e==="twoWay"}var rc=class extends Rv{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(e,t,i){super(),this._rootLView=t,this._hasInputBindings=i,this._tNode=Im(t[Re],Bt),this.location=ls(this._tNode,t),this.instance=Ri(this._tNode.index,t)[sn],this.hostView=this.changeDetectorRef=new as(t,void 0),this.componentType=e}setInput(e,t){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(e)&&Object.is(this.previousInputValues.get(e),t))return;let r=this._rootLView,s=gv(i,r[Re],r,e,t);this.previousInputValues.set(e,t);let o=Ri(i.index,r);Iv(o,1)}get injector(){return new hr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(e){this.hostView.onDestroy(e)}};function xS(n,e,t){let i=n.projection=[];for(let r=0;r<e.length;r++){let s=t[r];i.push(s!=null&&s.length?Array.from(s):null)}}var gc=(()=>{class n{static __NG_ELEMENT_ID__=MS}return n})();function MS(){let n=ci();return Bv(n,Ct())}var ff=class n extends gc{_lContainer;_hostTNode;_hostLView;constructor(e,t,i){super(),this._lContainer=e,this._hostTNode=t,this._hostLView=i}get element(){return ls(this._hostTNode,this._hostLView)}get injector(){return new hr(this._hostTNode,this._hostLView)}get parentInjector(){let e=Sf(this._hostTNode,this._hostLView);if(Ag(e)){let t=Qa(e,this._hostLView),i=Ka(e),r=t[Re].data[i+8];return new hr(r,t)}else return new hr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(e){let t=vg(this._lContainer);return t!==null&&t[e]||null}get length(){return this._lContainer.length-jt}createEmbeddedView(e,t,i){let r,s;typeof i=="number"?r=i:i!=null&&(r=i.index,s=i.injector);let o=af(this._lContainer,e.ssrId),a=e.createEmbeddedViewImpl(t||{},s,o);return this.insertImpl(a,r,rf(this._hostTNode,o)),a}createComponent(e,t,i,r,s,o,a){let c=e&&!f0(e),l;if(c)l=t;else{let m=t||{};l=m.index,i=m.injector,r=m.projectableNodes,s=m.environmentInjector||m.ngModuleRef,o=m.directives,a=m.bindings}let u=c?e:new lo(rr(e)),f=i||this.parentInjector;if(!s&&u.ngModule==null){let p=(c?f:this.parentInjector).get(gn,null);p&&(s=p)}let d=rr(u.componentType??{}),h=af(this._lContainer,d?.id??null),v=h?.firstChild??null,M=u.create(f,r,v,s,o,a);return this.insertImpl(M.hostView,l,rf(this._hostTNode,h)),M}insert(e,t){return this.insertImpl(e,t,!0)}insertImpl(e,t,i){let r=e._lView;if(Am(r)){let a=this.indexOf(e);if(a!==-1)this.detach(a);else{let c=r[Rt],l=new n(c,c[Wt],c[Rt]);l.detach(l.indexOf(e))}}let s=this._adjustIndex(t),o=this._lContainer;return Av(o,r,s,i),e.attachToViewContainerRef(),fd(qd(o),s,e),e}move(e,t){return this.insert(e,t)}indexOf(e){let t=vg(this._lContainer);return t!==null?t.indexOf(e):-1}remove(e){let t=this._adjustIndex(e,-1),i=of(this._lContainer,t);i&&($s(qd(this._lContainer),t),av(i[Re],i))}detach(e){let t=this._adjustIndex(e,-1),i=of(this._lContainer,t);return i&&$s(qd(this._lContainer),t)!=null?new as(i):null}_adjustIndex(e,t=0){return e??this.length+t}};function vg(n){return n[Js]}function qd(n){return n[Js]||(n[Js]=[])}function Bv(n,e){let t,i=e[n.index];return In(i)?t=i:(t=Cv(i,e,null,n),e[n.index]=t,Cf(e,t)),ES(t,e,n,i),new ff(t,n,e)}function SS(n,e){let t=n[Ut],i=t.createComment(""),r=Di(e,n),s=t.parentNode(r);return nc(t,s,i,t.nextSibling(r),!1),i}var ES=wS,bS=()=>!1;function TS(n,e,t){return bS(n,e,t)}function wS(n,e,t,i){if(n[Ci])return;let r;t.type&8?r=Cn(i):r=SS(e,t),n[Ci]=r}var hf=class n{queryList;matches=null;constructor(e){this.queryList=e}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},pf=class n{queries;constructor(e=[]){this.queries=e}createEmbeddedView(e){let t=e.queries;if(t!==null){let i=e.contentQueries!==null?e.contentQueries[0]:t.length,r=[];for(let s=0;s<i;s++){let o=t.getByIndex(s),a=this.queries[o.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(e){this.dirtyQueriesWithMatches(e)}detachView(e){this.dirtyQueriesWithMatches(e)}finishViewCreation(e){this.dirtyQueriesWithMatches(e)}dirtyQueriesWithMatches(e){for(let t=0;t<this.queries.length;t++)kf(e,t).matches!==null&&this.queries[t].setDirty()}},mf=class{flags;read;predicate;constructor(e,t,i=null){this.flags=t,this.read=i,typeof e=="string"?this.predicate=LS(e):this.predicate=e}},gf=class n{queries;constructor(e=[]){this.queries=e}elementStart(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(e,t)}elementEnd(e){for(let t=0;t<this.queries.length;t++)this.queries[t].elementEnd(e)}embeddedTView(e){let t=null;for(let i=0;i<this.length;i++){let r=t!==null?t.length:0,s=this.getByIndex(i).embeddedTView(e,r);s&&(s.indexInDeclarationView=i,t!==null?t.push(s):t=[s])}return t!==null?new n(t):null}template(e,t){for(let i=0;i<this.queries.length;i++)this.queries[i].template(e,t)}getByIndex(e){return this.queries[e]}get length(){return this.queries.length}track(e){this.queries.push(e)}},vf=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(e,t=-1){this.metadata=e,this._declarationNodeIndex=t}elementStart(e,t){this.isApplyingToNode(t)&&this.matchTNode(e,t)}elementEnd(e){this._declarationNodeIndex===e.index&&(this._appliesToNextNode=!1)}template(e,t){this.elementStart(e,t)}embeddedTView(e,t){return this.isApplyingToNode(e)?(this.crossesNgTemplate=!0,this.addMatch(-e.index,t),new n(this.metadata)):null}isApplyingToNode(e){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let t=this._declarationNodeIndex,i=e.parent;for(;i!==null&&i.type&8&&i.index!==t;)i=i.parent;return t===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(e,t){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let s=i[r];this.matchTNodeWithReadOption(e,t,IS(t,s)),this.matchTNodeWithReadOption(e,t,Ya(t,e,s,!1,!1))}else i===co?t.type&4&&this.matchTNodeWithReadOption(e,t,-1):this.matchTNodeWithReadOption(e,t,Ya(t,e,i,!1,!1))}matchTNodeWithReadOption(e,t,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===ho||r===gc||r===co&&t.type&4)this.addMatch(t.index,-2);else{let s=Ya(t,e,r,!1,!1);s!==null&&this.addMatch(t.index,s)}else this.addMatch(t.index,i)}}addMatch(e,t){this.matches===null?this.matches=[e,t]:this.matches.push(e,t)}};function IS(n,e){let t=n.localNames;if(t!==null){for(let i=0;i<t.length;i+=2)if(t[i]===e)return t[i+1]}return null}function CS(n,e){return n.type&11?ls(n,e):n.type&4?Ff(n,e):null}function AS(n,e,t,i){return t===-1?CS(e,n):t===-2?DS(n,e,i):ec(n,n[Re],t,e)}function DS(n,e,t){if(t===ho)return ls(e,n);if(t===co)return Ff(e,n);if(t===gc)return Bv(e,n)}function Vv(n,e,t,i){let r=e[Wn].queries[i];if(r.matches===null){let s=n.data,o=t.matches,a=[];for(let c=0;o!==null&&c<o.length;c+=2){let l=o[c];if(l<0)a.push(null);else{let u=s[l];a.push(AS(e,u,o[c+1],t.metadata.read))}}r.matches=a}return r.matches}function yf(n,e,t,i){let r=n.queries.getByIndex(t),s=r.matches;if(s!==null){let o=Vv(n,e,r,t);for(let a=0;a<s.length;a+=2){let c=s[a];if(c>0)i.push(o[a/2]);else{let l=s[a+1],u=e[-c];for(let f=jt;f<u.length;f++){let d=u[f];d[wi]===d[Rt]&&yf(d[Re],d,l,i)}if(u[cr]!==null){let f=u[cr];for(let d=0;d<f.length;d++){let h=f[d];yf(h[Re],h,l,i)}}}}}return i}function RS(n,e){return n[Wn].queries[e].queryList}function NS(n,e,t){let i=new tc((t&4)===4);return Pm(n,e,i,i.destroy),(e[Wn]??=new pf).queries.push(new hf(i))-1}function PS(n,e,t){let i=dr();return i.firstCreatePass&&(OS(i,new mf(n,e,t),-1),(e&2)===2&&(i.staticViewQueries=!0)),NS(i,Ct(),e)}function LS(n){return n.split(",").map(e=>e.trim())}function OS(n,e,t){n.queries===null&&(n.queries=new gf),n.queries.track(new vf(e,t))}function kf(n,e){return n.queries.getByIndex(e)}function FS(n,e){let t=n[Re],i=kf(t,e);return i.crossesNgTemplate?yf(t,n,e,[]):Vv(t,n,i,e)}var sc=class{};var uo=class extends sc{injector;componentFactoryResolver=new df(this);instance=null;constructor(e){super();let t=new tr([...e.providers,{provide:sc,useValue:this},{provide:mc,useValue:this.componentFactoryResolver}],e.parent||Xs(),e.debugName,new Set(["environment"]));this.injector=t,e.runEnvironmentInitializers&&t.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(e){this.injector.onDestroy(e)}};function Hv(n,e,t=null){return new uo({providers:n,parent:e,debugName:t,runEnvironmentInitializers:!0}).injector}var kS=(()=>{class n{_injector;cachedInjectors=new Map;constructor(t){this._injector=t}getOrCreateStandaloneInjector(t){if(!t.standalone)return null;if(!this.cachedInjectors.has(t)){let i=md(!1,t.type),r=i.length>0?Hv([i],this._injector,""):null;this.cachedInjectors.set(t,r)}return this.cachedInjectors.get(t)}ngOnDestroy(){try{for(let t of this.cachedInjectors.values())t!==null&&t.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=Et({token:n,providedIn:"environment",factory:()=>new n(at(gn))})}return n})();function yr(n){return bg(()=>{let e=HS(n),t=Xt(Lt({},e),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Ef.OnPush,directiveDefs:null,pipeDefs:null,dependencies:e.standalone&&n.dependencies||null,getStandaloneInjector:e.standalone?r=>r.get(kS).getOrCreateStandaloneInjector(t):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||Rn.Emulated,styles:n.styles||zn,_:null,schemas:n.schemas||null,tView:null,id:""});e.standalone&&iv("NgStandalone"),zS(t);let i=n.dependencies;return t.directiveDefs=yg(i,US),t.pipeDefs=yg(i,um),t.id=GS(t),t})}function US(n){return rr(n)||cd(n)}function BS(n,e){if(n==null)return sr;let t={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],s,o,a,c;Array.isArray(r)?(a=r[0],s=r[1],o=r[2]??s,c=r[3]||null):(s=r,o=r,a=fc.None,c=null),t[s]=[i,a,c],e[s]=o}return t}function VS(n){if(n==null)return sr;let e={};for(let t in n)n.hasOwnProperty(t)&&(e[n[t]]=t);return e}function HS(n){let e={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:e,inputConfig:n.inputs||sr,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||zn,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:BS(n.inputs,e),outputs:VS(n.outputs),debugInfo:null}}function zS(n){n.features?.forEach(e=>e(n))}function yg(n,e){return n?()=>{let t=typeof n=="function"?n():n,i=[];for(let r of t){let s=e(r);s!==null&&i.push(s)}return i}:null}function GS(n){let e=0,t=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,t,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let s of i.join("|"))e=Math.imul(31,e)+s.charCodeAt(0)<<0;return e+=2147483648,"c"+e}function WS(n,e,t,i,r,s,o,a){if(t.firstCreatePass){n.mergedAttrs=ac(n.mergedAttrs,n.attrs);let u=n.tView=wf(2,n,r,s,o,t.directiveRegistry,t.pipeRegistry,null,t.schemas,t.consts,null);t.queries!==null&&(t.queries.template(t,n),u.queries=t.queries.embeddedTView(n))}a&&(n.flags|=a),es(n,!1);let c=$S(t,e,n,i);Wa()&&Nf(t,e,c,n),ss(c,e);let l=Cv(c,e,c,n);e[i+Bt]=l,Cf(e,l),TS(l,n,e)}function jS(n,e,t,i,r,s,o,a,c,l,u){let f=t+Bt,d;if(e.firstCreatePass){if(d=po(e,f,4,o||null,a||null),l!=null){let h=Qr(e.consts,l);d.localNames=[];for(let v=0;v<h.length;v+=2)d.localNames.push(h[v],-1)}}else d=e.data[f];return WS(d,n,e,t,i,r,s,c),l!=null&&Lf(n,d,u),d}var $S=qS;function qS(n,e,t,i){return ja(!0),e[Ut].createComment("")}var Uf=new Ke("");function Bf(n){return!!n&&typeof n.then=="function"}function zv(n){return!!n&&typeof n.subscribe=="function"}var Gv=new Ke("");var Vf=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((t,i)=>{this.resolve=t,this.reject=i});appInits=$e(Gv,{optional:!0})??[];injector=$e(nr);constructor(){}runInitializers(){if(this.initialized)return;let t=[];for(let r of this.appInits){let s=La(this.injector,r);if(Bf(s))t.push(s);else if(zv(s)){let o=new Promise((a,c)=>{s.subscribe({complete:a,error:c})});t.push(o)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(t).then(()=>{i()}).catch(r=>{this.reject(r)}),t.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Et({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Wv=new Ke("");function jv(){Au(()=>{let n="";throw new Ge(600,n)})}function $v(n){return n.isBoundToModule}var XS=10;var vc=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=$e(ns);afterRenderManager=$e(rv);zonelessEnabled=$e(no);rootEffectScheduler=$e(Gd);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new ri;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=$e(ts);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(Bu(t=>!t))}constructor(){$e(us,{optional:!0})}whenStable(){let t;return new Promise(i=>{t=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{t.unsubscribe()})}_injector=$e(gn);_rendererFactory=null;get injector(){return this._injector}bootstrap(t,i){return this.bootstrapImpl(t,i)}bootstrapImpl(t,i,r=nr.NULL){return this._injector.get(Zt).run(()=>{ft(et.BootstrapComponentStart);let o=t instanceof pc;if(!this._injector.get(Vf).done){let v="";throw new Ge(405,v)}let c;o?c=t:c=this._injector.get(mc).resolveComponentFactory(t),this.componentTypes.push(c.componentType);let l=$v(c)?void 0:this._injector.get(sc),u=i||c.selector,f=c.create(r,[],u,l),d=f.location.nativeElement,h=f.injector.get(Uf,null);return h?.registerApplication(d),f.onDestroy(()=>{this.detachView(f.hostView),so(this.components,f),h?.unregisterApplication(d)}),this._loadComponent(f),ft(et.BootstrapComponentEnd,f),f})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){ft(et.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(Df.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw ft(et.ChangeDetectionEnd),new Ge(101,!1);let t=je(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,je(t),this.afterTick.next(),ft(et.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(pr,null,{optional:!0}));let t=0;for(;this.dirtyFlags!==0&&t++<XS;){ft(et.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{ft(et.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let t=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Ks(r))continue;let s=i&&!this.zonelessEnabled?0:1;Ev(r,s),t=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}t||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:t})=>Ks(t))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(t){let i=t;this._views.push(i),i.attachToAppRef(this)}detachView(t){let i=t;so(this._views,i),i.detachFromAppRef()}_loadComponent(t){this.attachView(t.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(t),this._injector.get(Wv,[]).forEach(r=>r(t))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(t=>t()),this._views.slice().forEach(t=>t.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(t){return this._destroyListeners.push(t),()=>so(this._destroyListeners,t)}destroy(){if(this._destroyed)throw new Ge(406,!1);let t=this._injector;t.destroy&&!t.destroyed&&t.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Et({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function so(n,e){let t=n.indexOf(e);t>-1&&n.splice(t,1)}function _f(n,e,t,i,r){gv(e,n,t,r?"class":"style",i)}function _n(n,e,t,i){let r=Ct(),s=r[Re],o=n+Bt,a=s.firstCreatePass?Ov(o,r,2,e,NM,Fm(),t,i):s.data[o];if(Jr(a)){let c=r[Gn].tracingService;if(c&&c.componentCreate){let l=s.data[a.directiveStart+a.componentOffset];return c.componentCreate(kv(l),()=>(_g(n,e,r,a,i),_n))}}return _g(n,e,r,a,i),_n}function _g(n,e,t,i,r){if(pv(i,t,n,e,qv),xd(i)){let s=t[Re];hv(s,t,i),Xg(s,i,t)}r!=null&&Lf(t,i)}function $n(){let n=dr(),e=ci(),t=mv(e);return n.firstCreatePass&&Fv(n,t),Id(t)&&Cd(),Td(),t.classesWithoutHost!=null&&x0(t)&&_f(n,t,Ct(),t.classesWithoutHost,!0),t.stylesWithoutHost!=null&&M0(t)&&_f(n,t,Ct(),t.stylesWithoutHost,!1),$n}function ds(n,e,t,i){return _n(n,e,t,i),$n(),ds}function _r(n,e,t,i){let r=Ct(),s=r[Re],o=n+Bt,a=s.firstCreatePass?dS(o,s,2,e,t,i):s.data[o];return pv(a,r,n,e,qv),i!=null&&Lf(r,a),_r}function fs(){let n=ci(),e=mv(n);return Id(e)&&Cd(),Td(),fs}function xr(n,e,t,i){return _r(n,e,t,i),fs(),xr}var qv=(n,e,t,i,r)=>(ja(!0),Zg(e[Ut],i,kd()));var mo="en-US";var YS=mo;function Xv(n){typeof n=="string"&&(YS=n.toLowerCase().replace(/_/g,"-"))}function ZS(n,e){let t=null,i=K0(n);for(let r=0;r<e.length;r++){let s=e[r];if(s==="*"){t=r;continue}if(i===null?ev(n,s,!0):tM(i,s))return r}return t}function Hf(n){let e=Ct()[on][Wt];if(!e.projection){let t=n?n.length:1,i=e.projection=gm(t,null),r=i.slice(),s=e.child;for(;s!==null;){if(s.type!==128){let o=n?ZS(s,n):0;o!==null&&(r[o]?r[o].projectionNext=s:i[o]=s,r[o]=s)}s=s.next}}}function zf(n,e=0,t,i,r,s){let o=Ct(),a=dr(),c=i?n+1:null;c!==null&&jS(o,a,c,i,r,s,null,t);let l=po(a,Bt+n,16,null,t||null);l.projection===null&&(l.projection=e),Rd();let f=!o[qr]||wd();o[on][Wt].projection[l.projection]===null&&c!==null?JS(o,a,c):f&&!dc(l)&&EM(a,o,l)}function JS(n,e,t){let i=Bt+t,r=e.data[i],s=n[i],o=af(s,r.tView.ssrId),a=vv(n,r,void 0,{dehydratedView:o});Av(s,a,0,rf(r,o))}function yc(n,e,t){return PS(n,e,t),yc}function Gf(n){let e=Ct(),t=dr(),i=Ld();Va(i+1);let r=kf(t,i);if(n.dirty&&Cm(e)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let s=FS(e,i);n.reset(s,O0),n.notifyOnChanges()}return!0}return!1}function Wf(){return RS(Ct(),Ld())}function $a(n,e){return n<<17|e<<2}function mr(n){return n>>17&32767}function KS(n){return(n&2)==2}function QS(n,e){return n&131071|e<<17}function xf(n){return n|2}function cs(n){return(n&131068)>>2}function Xd(n,e){return n&-131069|e<<2}function eE(n){return(n&1)===1}function Mf(n){return n|1}function tE(n,e,t,i,r,s){let o=s?e.classBindings:e.styleBindings,a=mr(o),c=cs(o);n[i]=t;let l=!1,u;if(Array.isArray(t)){let f=t;u=f[1],(u===null||jr(f,u)>0)&&(l=!0)}else u=t;if(r)if(c!==0){let d=mr(n[a+1]);n[i+1]=$a(d,a),d!==0&&(n[d+1]=Xd(n[d+1],i)),n[a+1]=QS(n[a+1],i)}else n[i+1]=$a(a,0),a!==0&&(n[a+1]=Xd(n[a+1],i)),a=i;else n[i+1]=$a(c,0),a===0?a=i:n[c+1]=Xd(n[c+1],i),c=i;l&&(n[i+1]=xf(n[i+1])),xg(n,u,i,!0),xg(n,u,i,!1),nE(e,u,n,i,s),o=$a(a,c),s?e.classBindings=o:e.styleBindings=o}function nE(n,e,t,i,r){let s=r?n.residualClasses:n.residualStyles;s!=null&&typeof e=="string"&&jr(s,e)>=0&&(t[i+1]=Mf(t[i+1]))}function xg(n,e,t,i){let r=n[t+1],s=e===null,o=i?mr(r):cs(r),a=!1;for(;o!==0&&(a===!1||s);){let c=n[o],l=n[o+1];iE(c,e)&&(a=!0,n[o+1]=i?Mf(l):xf(l)),o=i?mr(l):cs(l)}a&&(n[t+1]=i?xf(r):Mf(r))}function iE(n,e){return n===null||e==null||(Array.isArray(n)?n[1]:n)===e?!0:Array.isArray(n)&&typeof e=="string"?jr(n,e)>=0:!1}var Dn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function rE(n){return n.substring(Dn.key,Dn.keyEnd)}function sE(n){return oE(n),Yv(n,Zv(n,0,Dn.textEnd))}function Yv(n,e){let t=Dn.textEnd;return t===e?-1:(e=Dn.keyEnd=aE(n,Dn.key=e,t),Zv(n,e,t))}function oE(n){Dn.key=0,Dn.keyEnd=0,Dn.value=0,Dn.valueEnd=0,Dn.textEnd=n.length}function Zv(n,e,t){for(;e<t&&n.charCodeAt(e)<=32;)e++;return e}function aE(n,e,t){for(;e<t&&n.charCodeAt(e)>32;)e++;return e}function jf(n){lE(gE,cE,n,!0)}function cE(n,e){for(let t=sE(e);t>=0;t=Yv(e,t))Ra(n,rE(e),!0)}function lE(n,e,t,i){let r=dr(),s=Bm(2);r.firstUpdatePass&&uE(r,null,s,i);let o=Ct();if(t!==vr&&fS(o,s,t)){let a=r.data[eo()];if(Kv(a,i)&&!Jv(r,s)){let c=i?a.classesWithoutHost:a.stylesWithoutHost;c!==null&&(t=Ta(c,t||"")),_f(r,a,o,t,i)}else vE(r,a,o,o[Ut],o[s+1],o[s+1]=mE(n,e,t),i,s)}}function Jv(n,e){return e>=n.expandoStartIndex}function uE(n,e,t,i){let r=n.data;if(r[t+1]===null){let s=r[eo()],o=Jv(n,t);Kv(s,i)&&e===null&&!o&&(e=!1),e=dE(r,s,e,i),tE(r,s,e,t,o,i)}}function dE(n,e,t,i){let r=Gm(n),s=i?e.residualClasses:e.residualStyles;if(r===null)(i?e.classBindings:e.styleBindings)===0&&(t=Yd(null,n,e,t,i),t=fo(t,e.attrs,i),s=null);else{let o=e.directiveStylingLast;if(o===-1||n[o]!==r)if(t=Yd(r,n,e,t,i),s===null){let c=fE(n,e,i);c!==void 0&&Array.isArray(c)&&(c=Yd(null,n,e,c[1],i),c=fo(c,e.attrs,i),hE(n,e,i,c))}else s=pE(n,e,i)}return s!==void 0&&(i?e.residualClasses=s:e.residualStyles=s),t}function fE(n,e,t){let i=t?e.classBindings:e.styleBindings;if(cs(i)!==0)return n[mr(i)]}function hE(n,e,t,i){let r=t?e.classBindings:e.styleBindings;n[mr(r)]=i}function pE(n,e,t){let i,r=e.directiveEnd;for(let s=1+e.directiveStylingLast;s<r;s++){let o=n[s].hostAttrs;i=fo(i,o,t)}return fo(i,e.attrs,t)}function Yd(n,e,t,i,r){let s=null,o=t.directiveEnd,a=t.directiveStylingLast;for(a===-1?a=t.directiveStart:a++;a<o&&(s=e[a],i=fo(i,s.hostAttrs,r),s!==n);)a++;return n!==null&&(t.directiveStylingLast=a),i}function fo(n,e,t){let i=t?1:2,r=-1;if(e!==null)for(let s=0;s<e.length;s++){let o=e[s];typeof o=="number"?r=o:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Ra(n,o,t?!0:e[++s]))}return n===void 0?null:n}function mE(n,e,t){if(t==null||t==="")return zn;let i=[],r=Yg(t);if(Array.isArray(r))for(let s=0;s<r.length;s++)n(i,r[s],!0);else if(r instanceof Set)for(let s of r)n(i,s,!0);else if(typeof r=="object")for(let s in r)r.hasOwnProperty(s)&&n(i,s,r[s]);else typeof r=="string"&&e(i,r);return i}function gE(n,e,t){let i=String(e);i!==""&&!i.includes(" ")&&Ra(n,i,t)}function vE(n,e,t,i,r,s,o,a){r===vr&&(r=zn);let c=0,l=0,u=0<r.length?r[0]:null,f=0<s.length?s[0]:null;for(;u!==null||f!==null;){let d=c<r.length?r[c+1]:void 0,h=l<s.length?s[l+1]:void 0,v=null,M;u===f?(c+=2,l+=2,d!==h&&(v=f,M=h)):f===null||u!==null&&u<f?(c+=2,v=u):(l+=2,v=f,M=h),v!==null&&yE(n,e,t,i,v,M,o,a),u=c<r.length?r[c]:null,f=l<s.length?s[l]:null}}function yE(n,e,t,i,r,s,o,a){if(!(e.type&3))return;let c=n.data,l=c[a+1],u=eE(l)?Mg(c,e,t,r,cs(l),o):void 0;if(!oc(u)){oc(s)||KS(l)&&(s=Mg(c,null,t,r,a,o));let f=wm(eo(),t);TM(i,o,f,r,s)}}function Mg(n,e,t,i,r,s){let o=e===null,a;for(;r>0;){let c=n[r],l=Array.isArray(c),u=l?c[1]:c,f=u===null,d=t[r+1];d===vr&&(d=f?zn:void 0);let h=f?Na(d,i):u===i?d:void 0;if(l&&!oc(h)&&(h=Na(c,i)),oc(h)&&(a=h,o))return a;let v=n[r+1];r=o?mr(v):cs(v)}if(e!==null){let c=s?e.residualClasses:e.residualStyles;c!=null&&(a=Na(c,i))}return a}function oc(n){return n!==void 0}function Kv(n,e){return(n.flags&(e?8:16))!==0}function hs(n,e=""){let t=Ct(),i=dr(),r=n+Bt,s=i.firstCreatePass?po(i,r,1,e,null):i.data[r],o=_E(i,t,s,e);t[r]=o,Wa()&&Nf(i,t,o,s),es(s,!1)}var _E=(n,e,t,i)=>(ja(!0),G0(e[Ut],i));var Qv=(()=>{class n{applicationErrorHandler=$e(ns);appRef=$e(vc);taskService=$e(ts);ngZone=$e(Zt);zonelessEnabled=$e(no);tracing=$e(us,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new Gt;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ws):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&($e(zd,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let t=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(t);return}this.switchToMicrotaskScheduler(),this.taskService.remove(t)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let t=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(t)})})}notify(t){if(!this.zonelessEnabled&&t===5)return;switch(t){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?Jm:Ud;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ws+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let t=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(t),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let t=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(t)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Et({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function ey(){return[{provide:Wr,useExisting:Qv},{provide:Zt,useClass:js},{provide:no,useValue:!0}]}function xE(){return typeof $localize<"u"&&$localize.locale||mo}var $f=new Ke("",{factory:()=>$e($f,{optional:!0,skipSelf:!0})||xE()});var ny=Symbol("InputSignalNode#UNSET"),wE=Xt(Lt({},Ru),{transformFn:void 0,applyValueToInputSignal(n,e){Du(n,e)}});function iy(n,e){let t=Object.create(wE);t.value=n,t.transformFn=e?.transform;function i(){if(Tu(t),t.value===ny){let r=null;throw new Ge(-950,r)}return t.value}return i[bi]=t,i}function ty(n,e){return iy(n,e)}function IE(n){return iy(ny,n)}var Yf=(ty.required=IE,ty);var qf=new Ke(""),CE=new Ke("");function go(n){return!n.moduleRef}function AE(n){let e=go(n)?n.r3Injector:n.moduleRef.injector,t=e.get(Zt);return t.run(()=>{go(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=e.get(ns),r;if(t.runOutsideAngular(()=>{r=t.onError.subscribe({next:i})}),go(n)){let s=()=>e.destroy(),o=n.platformInjector.get(qf);o.add(s),e.onDestroy(()=>{r.unsubscribe(),o.delete(s)})}else{let s=()=>n.moduleRef.destroy(),o=n.platformInjector.get(qf);o.add(s),n.moduleRef.onDestroy(()=>{so(n.allPlatformModules,n.moduleRef),r.unsubscribe(),o.delete(s)})}return RE(i,t,()=>{let s=e.get(ts),o=s.add(),a=e.get(Vf);return a.runInitializers(),a.donePromise.then(()=>{let c=e.get($f,mo);if(Xv(c||mo),!e.get(CE,!0))return go(n)?e.get(vc):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(go(n)){let u=e.get(vc);return n.rootComponent!==void 0&&u.bootstrap(n.rootComponent),u}else return DE?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{s.remove(o)})})})}var DE;function RE(n,e,t){try{let i=t();return Bf(i)?i.catch(r=>{throw e.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw e.runOutsideAngular(()=>n(i)),i}}var _c=null;function NE(n=[],e){return nr.create({name:e,providers:[{provide:qs,useValue:"platform"},{provide:qf,useValue:new Set([()=>_c=null])},...n]})}function PE(n=[]){if(_c)return _c;let e=NE(n);return _c=e,jv(),LE(e),e}function LE(n){let e=n.get(lc,null);La(n,()=>{e?.forEach(t=>t())})}var OE=1e4;var Jk=OE-1e3;function ry(n){let{rootComponent:e,appProviders:t,platformProviders:i,platformRef:r}=n;ft(et.BootstrapApplicationStart);try{let s=r?.injector??PE(i),o=[ey(),Qm,...t||[]],a=new uo({providers:o,parent:s,debugName:"",runEnvironmentInitializers:!1});return AE({r3Injector:a.injector,platformInjector:s,rootComponent:e})}catch(s){return Promise.reject(s)}finally{ft(et.BootstrapApplicationEnd)}}var sy=null;function _o(){return sy}function Zf(n){sy??=n}var yo=class{};function Jf(n,e){e=encodeURIComponent(e);for(let t of n.split(";")){let i=t.indexOf("="),[r,s]=i==-1?[t,""]:[t.slice(0,i),t.slice(i+1)];if(r.trim()===e)return decodeURIComponent(s)}return null}var xo=class{};var Kf="browser";function Qf(n){return n===Kf}var Mo=class{_doc;constructor(e){this._doc=e}manager},xc=(()=>{class n extends Mo{constructor(t){super(t)}supports(t){return!0}addEventListener(t,i,r,s){return t.addEventListener(i,r,s),()=>this.removeEventListener(t,i,r,s)}removeEventListener(t,i,r,s){return t.removeEventListener(i,r,s)}static \u0275fac=function(i){return new(i||n)(at(yn))};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})(),Ec=new Ke(""),ih=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(t,i){this._zone=i,t.forEach(o=>{o.manager=this});let r=t.filter(o=>!(o instanceof xc));this._plugins=r.slice().reverse();let s=t.find(o=>o instanceof xc);s&&this._plugins.push(s)}addEventListener(t,i,r,s){return this._findPluginFor(i).addEventListener(t,i,r,s)}getZone(){return this._zone}_findPluginFor(t){let i=this._eventNameToPlugin.get(t);if(i)return i;if(i=this._plugins.find(s=>s.supports(t)),!i)throw new Ge(5101,!1);return this._eventNameToPlugin.set(t,i),i}static \u0275fac=function(i){return new(i||n)(at(Ec),at(Zt))};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})(),eh="ng-app-id";function oy(n){for(let e of n)e.remove()}function ay(n,e){let t=e.createElement("style");return t.textContent=n,t}function FE(n,e,t,i){let r=n.head?.querySelectorAll(`style[${eh}="${e}"],link[${eh}="${e}"]`);if(r)for(let s of r)s.removeAttribute(eh),s instanceof HTMLLinkElement?i.set(s.href.slice(s.href.lastIndexOf("/")+1),{usage:0,elements:[s]}):s.textContent&&t.set(s.textContent,{usage:0,elements:[s]})}function nh(n,e){let t=e.createElement("link");return t.setAttribute("rel","stylesheet"),t.setAttribute("href",n),t}var rh=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(t,i,r,s={}){this.doc=t,this.appId=i,this.nonce=r,FE(t,i,this.inline,this.external),this.hosts.add(t.head)}addStyles(t,i){for(let r of t)this.addUsage(r,this.inline,ay);i?.forEach(r=>this.addUsage(r,this.external,nh))}removeStyles(t,i){for(let r of t)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(t,i,r){let s=i.get(t);s?s.usage++:i.set(t,{usage:1,elements:[...this.hosts].map(o=>this.addElement(o,r(t,this.doc)))})}removeUsage(t,i){let r=i.get(t);r&&(r.usage--,r.usage<=0&&(oy(r.elements),i.delete(t)))}ngOnDestroy(){for(let[,{elements:t}]of[...this.inline,...this.external])oy(t);this.hosts.clear()}addHost(t){this.hosts.add(t);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(t,ay(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(t,nh(i,this.doc)))}removeHost(t){this.hosts.delete(t)}addElement(t,i){return this.nonce&&i.setAttribute("nonce",this.nonce),t.appendChild(i)}static \u0275fac=function(i){return new(i||n)(at(yn),at(cc),at(uc,8),at(gr))};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})(),th={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},sh=/%COMP%/g;var ly="%COMP%",kE=`_nghost-${ly}`,UE=`_ngcontent-${ly}`,BE=!0,VE=new Ke("",{factory:()=>BE});function HE(n){return UE.replace(sh,n)}function zE(n){return kE.replace(sh,n)}function uy(n,e){return e.map(t=>t.replace(sh,n))}var oh=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(t,i,r,s,o,a,c=null,l=null){this.eventManager=t,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=s,this.doc=o,this.ngZone=a,this.nonce=c,this.tracingService=l,this.defaultRenderer=new So(t,o,a,this.tracingService)}createRenderer(t,i){if(!t||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(t,i);return r instanceof Sc?r.applyToHost(t):r instanceof Eo&&r.applyStyles(),r}getOrCreateRenderer(t,i){let r=this.rendererByCompId,s=r.get(i.id);if(!s){let o=this.doc,a=this.ngZone,c=this.eventManager,l=this.sharedStylesHost,u=this.removeStylesOnCompDestroy,f=this.tracingService;switch(i.encapsulation){case Rn.Emulated:s=new Sc(c,l,i,this.appId,u,o,a,f);break;case Rn.ShadowDom:return new Mc(c,t,i,o,a,this.nonce,f,l);case Rn.ExperimentalIsolatedShadowDom:return new Mc(c,t,i,o,a,this.nonce,f);default:s=new Eo(c,l,i,u,o,a,f);break}r.set(i.id,s)}return s}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(t){this.rendererByCompId.delete(t)}static \u0275fac=function(i){return new(i||n)(at(ih),at(rh),at(cc),at(VE),at(yn),at(Zt),at(uc),at(us,8))};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})(),So=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(e,t,i,r){this.eventManager=e,this.doc=t,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(e,t){return t?this.doc.createElementNS(th[t]||t,e):this.doc.createElement(e)}createComment(e){return this.doc.createComment(e)}createText(e){return this.doc.createTextNode(e)}appendChild(e,t){(cy(e)?e.content:e).appendChild(t)}insertBefore(e,t,i){e&&(cy(e)?e.content:e).insertBefore(t,i)}removeChild(e,t){t.remove()}selectRootElement(e,t){let i=typeof e=="string"?this.doc.querySelector(e):e;if(!i)throw new Ge(-5104,!1);return t||(i.textContent=""),i}parentNode(e){return e.parentNode}nextSibling(e){return e.nextSibling}setAttribute(e,t,i,r){if(r){t=r+":"+t;let s=th[r];s?e.setAttributeNS(s,t,i):e.setAttribute(t,i)}else e.setAttribute(t,i)}removeAttribute(e,t,i){if(i){let r=th[i];r?e.removeAttributeNS(r,t):e.removeAttribute(`${i}:${t}`)}else e.removeAttribute(t)}addClass(e,t){e.classList.add(t)}removeClass(e,t){e.classList.remove(t)}setStyle(e,t,i,r){r&(li.DashCase|li.Important)?e.style.setProperty(t,i,r&li.Important?"important":""):e.style[t]=i}removeStyle(e,t,i){i&li.DashCase?e.style.removeProperty(t):e.style[t]=""}setProperty(e,t,i){e!=null&&(e[t]=i)}setValue(e,t){e.nodeValue=t}listen(e,t,i,r){if(typeof e=="string"&&(e=_o().getGlobalEventTarget(this.doc,e),!e))throw new Ge(5102,!1);let s=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(s=this.tracingService.wrapEventListener(e,t,s)),this.eventManager.addEventListener(e,t,s,r)}decoratePreventDefault(e){return t=>{if(t==="__ngUnwrap__")return e;e(t)===!1&&t.preventDefault()}}};function cy(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Mc=class extends So{hostEl;sharedStylesHost;shadowRoot;constructor(e,t,i,r,s,o,a,c){super(e,r,s,a),this.hostEl=t,this.sharedStylesHost=c,this.shadowRoot=t.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let l=i.styles;l=uy(i.id,l);for(let f of l){let d=document.createElement("style");o&&d.setAttribute("nonce",o),d.textContent=f,this.shadowRoot.appendChild(d)}let u=i.getExternalStyles?.();if(u)for(let f of u){let d=nh(f,r);o&&d.setAttribute("nonce",o),this.shadowRoot.appendChild(d)}}nodeOrShadowRoot(e){return e===this.hostEl?this.shadowRoot:e}appendChild(e,t){return super.appendChild(this.nodeOrShadowRoot(e),t)}insertBefore(e,t,i){return super.insertBefore(this.nodeOrShadowRoot(e),t,i)}removeChild(e,t){return super.removeChild(null,t)}parentNode(e){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(e)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Eo=class extends So{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(e,t,i,r,s,o,a,c){super(e,s,o,a),this.sharedStylesHost=t,this.removeStylesOnCompDestroy=r;let l=i.styles;this.styles=c?uy(c,l):l,this.styleUrls=i.getExternalStyles?.(c)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&os.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},Sc=class extends Eo{contentAttr;hostAttr;constructor(e,t,i,r,s,o,a,c){let l=r+"-"+i.id;super(e,t,i,s,o,a,c,l),this.contentAttr=HE(l),this.hostAttr=zE(l)}applyToHost(e){this.applyStyles(),this.setAttribute(e,this.hostAttr,"")}createElement(e,t){let i=super.createElement(e,t);return super.setAttribute(i,this.contentAttr,""),i}};var bc=class n extends yo{supportsDOMEvents=!0;static makeCurrent(){Zf(new n)}onAndCancel(e,t,i,r){return e.addEventListener(t,i,r),()=>{e.removeEventListener(t,i,r)}}dispatchEvent(e,t){e.dispatchEvent(t)}remove(e){e.remove()}createElement(e,t){return t=t||this.getDefaultDocument(),t.createElement(e)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(e){return e.nodeType===Node.ELEMENT_NODE}isShadowRoot(e){return e instanceof DocumentFragment}getGlobalEventTarget(e,t){return t==="window"?window:t==="document"?e:t==="body"?e.body:null}getBaseHref(e){let t=GE();return t==null?null:WE(t)}resetBaseElement(){bo=null}getUserAgent(){return window.navigator.userAgent}getCookie(e){return Jf(document.cookie,e)}},bo=null;function GE(){return bo=bo||document.head.querySelector("base"),bo?bo.getAttribute("href"):null}function WE(n){return new URL(n,document.baseURI).pathname}var jE=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})(),dy=["alt","control","meta","shift"],$E={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},qE={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},fy=(()=>{class n extends Mo{constructor(t){super(t)}supports(t){return n.parseEventName(t)!=null}addEventListener(t,i,r,s){let o=n.parseEventName(i),a=n.eventCallback(o.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>_o().onAndCancel(t,o.domEventName,a,s))}static parseEventName(t){let i=t.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let s=n._normalizeKey(i.pop()),o="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),o="code."),dy.forEach(l=>{let u=i.indexOf(l);u>-1&&(i.splice(u,1),o+=l+".")}),o+=s,i.length!=0||s.length===0)return null;let c={};return c.domEventName=r,c.fullKey=o,c}static matchEventFullKeyCode(t,i){let r=$E[t.key]||t.key,s="";return i.indexOf("code.")>-1&&(r=t.code,s="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),dy.forEach(o=>{if(o!==r){let a=qE[o];a(t)&&(s+=o+".")}}),s+=r,s===i)}static eventCallback(t,i,r){return s=>{n.matchEventFullKeyCode(s,t)&&r.runGuarded(()=>i(s))}}static _normalizeKey(t){return t==="esc"?"escape":t}static \u0275fac=function(i){return new(i||n)(at(yn))};static \u0275prov=Et({token:n,factory:n.\u0275fac})}return n})();async function ah(n,e,t){let i=Lt({rootComponent:n},XE(e,t));return ry(i)}function XE(n,e){return{platformRef:e?.platformRef,appProviders:[...QE,...n?.providers??[]],platformProviders:KE}}function YE(){bc.makeCurrent()}function ZE(){return new ai}function JE(){return bf(document),document}var KE=[{provide:gr,useValue:Kf},{provide:lc,useValue:YE,multi:!0},{provide:yn,useFactory:JE}];var QE=[{provide:qs,useValue:"root"},{provide:ai,useFactory:ZE},{provide:Ec,useClass:xc,multi:!0},{provide:Ec,useClass:fy,multi:!0},oh,rh,ih,{provide:pr,useExisting:oh},{provide:xo,useClass:jE},[]];var Ry=0,Vh=1,Ny=2;var Xo=1,Py=2,Ps=3,gi=0,qt=1,En=2,Qn=0,Ir=1,Hh=2,zh=3,Gh=4,Ly=5;var Vi=100,Oy=101,Fy=102,ky=103,Uy=104,By=200,Vy=201,Hy=202,zy=203,$c=204,qc=205,Gy=206,Wy=207,jy=208,$y=209,qy=210,Xy=211,Yy=212,Zy=213,Jy=214,Xc=0,Yc=1,Zc=2,Cr=3,Jc=4,Kc=5,Qc=6,el=7,Wh=0,Ky=1,Qy=2,Fn=0,jh=1,$h=2,qh=3,Xh=4,Yh=5,Zh=6,Jh=7;var Rh=300,ji=301,Dr=302,bl=303,Tl=304,Yo=306,tl=1e3,Yn=1001,nl=1002,Pt=1003,e_=1004;var Zo=1005;var Ot=1006,wl=1007;var $i=1008;var pn=1009,Kh=1010,Qh=1011,Ls=1012,Il=1013,kn=1014,Un=1015,ei=1016,Cl=1017,Al=1018,Os=1020,ep=35902,tp=35899,np=1021,ip=1022,bn=1023,Zn=1026,qi=1027,rp=1028,Dl=1029,Xi=1030,Rl=1031;var Nl=1033,Jo=33776,Ko=33777,Qo=33778,ea=33779,Pl=35840,Ll=35841,Ol=35842,Fl=35843,kl=36196,Ul=37492,Bl=37496,Vl=37488,Hl=37489,ta=37490,zl=37491,Gl=37808,Wl=37809,jl=37810,$l=37811,ql=37812,Xl=37813,Yl=37814,Zl=37815,Jl=37816,Kl=37817,Ql=37818,eu=37819,tu=37820,nu=37821,iu=36492,ru=36494,su=36495,ou=36283,au=36284,na=36285,cu=36286;var Do=2300,il=2301,jc=2302,Nh=2303,Ph=2400,Lh=2401,Oh=2402;var t_=3200;var sp=0,n_=1,_i="",un="srgb",Ro="srgb-linear",No="linear",Qe="srgb";var Tr=7680;var Fh=519,i_=512,r_=513,s_=514,lu=515,o_=516,a_=517,uu=518,c_=519,kh=35044;var op="300 es",On=2e3,Po=2001;function eb(n){for(let e=n.length-1;e>=0;--e)if(n[e]>=65535)return!0;return!1}function tb(n){return ArrayBuffer.isView(n)&&!(n instanceof DataView)}function Lo(n){return document.createElementNS("http://www.w3.org/1999/xhtml",n)}function l_(){let n=Lo("canvas");return n.style.display="block",n}var hy={},Is=null;function ap(...n){let e="THREE."+n.shift();Is?Is("log",e,...n):console.log(e,...n)}function u_(n){let e=n[0];if(typeof e=="string"&&e.startsWith("TSL:")){let t=n[1];t&&t.isStackTrace?n[0]+=" "+t.getLocation():n[1]='Stack trace not available. Enable "THREE.Node.captureStackTrace" to capture stack traces.'}return n}function be(...n){n=u_(n);let e="THREE."+n.shift();if(Is)Is("warn",e,...n);else{let t=n[0];t&&t.isStackTrace?console.warn(t.getError(e)):console.warn(e,...n)}}function we(...n){n=u_(n);let e="THREE."+n.shift();if(Is)Is("error",e,...n);else{let t=n[0];t&&t.isStackTrace?console.error(t.getError(e)):console.error(e,...n)}}function rl(...n){let e=n.join(" ");e in hy||(hy[e]=!0,be(...n))}function d_(n,e,t){return new Promise(function(i,r){function s(){switch(n.clientWaitSync(e,n.SYNC_FLUSH_COMMANDS_BIT,0)){case n.WAIT_FAILED:r();break;case n.TIMEOUT_EXPIRED:setTimeout(s,t);break;default:i()}}setTimeout(s,t)})}var f_={[Xc]:Yc,[Zc]:Qc,[Jc]:el,[Cr]:Kc,[Yc]:Xc,[Qc]:Zc,[el]:Jc,[Kc]:Cr},Jn=class{addEventListener(e,t){this._listeners===void 0&&(this._listeners={});let i=this._listeners;i[e]===void 0&&(i[e]=[]),i[e].indexOf(t)===-1&&i[e].push(t)}hasEventListener(e,t){let i=this._listeners;return i===void 0?!1:i[e]!==void 0&&i[e].indexOf(t)!==-1}removeEventListener(e,t){let i=this._listeners;if(i===void 0)return;let r=i[e];if(r!==void 0){let s=r.indexOf(t);s!==-1&&r.splice(s,1)}}dispatchEvent(e){let t=this._listeners;if(t===void 0)return;let i=t[e.type];if(i!==void 0){e.target=this;let r=i.slice(0);for(let s=0,o=r.length;s<o;s++)r[s].call(this,e);e.target=null}}},Vt=["00","01","02","03","04","05","06","07","08","09","0a","0b","0c","0d","0e","0f","10","11","12","13","14","15","16","17","18","19","1a","1b","1c","1d","1e","1f","20","21","22","23","24","25","26","27","28","29","2a","2b","2c","2d","2e","2f","30","31","32","33","34","35","36","37","38","39","3a","3b","3c","3d","3e","3f","40","41","42","43","44","45","46","47","48","49","4a","4b","4c","4d","4e","4f","50","51","52","53","54","55","56","57","58","59","5a","5b","5c","5d","5e","5f","60","61","62","63","64","65","66","67","68","69","6a","6b","6c","6d","6e","6f","70","71","72","73","74","75","76","77","78","79","7a","7b","7c","7d","7e","7f","80","81","82","83","84","85","86","87","88","89","8a","8b","8c","8d","8e","8f","90","91","92","93","94","95","96","97","98","99","9a","9b","9c","9d","9e","9f","a0","a1","a2","a3","a4","a5","a6","a7","a8","a9","aa","ab","ac","ad","ae","af","b0","b1","b2","b3","b4","b5","b6","b7","b8","b9","ba","bb","bc","bd","be","bf","c0","c1","c2","c3","c4","c5","c6","c7","c8","c9","ca","cb","cc","cd","ce","cf","d0","d1","d2","d3","d4","d5","d6","d7","d8","d9","da","db","dc","dd","de","df","e0","e1","e2","e3","e4","e5","e6","e7","e8","e9","ea","eb","ec","ed","ee","ef","f0","f1","f2","f3","f4","f5","f6","f7","f8","f9","fa","fb","fc","fd","fe","ff"];var ch=Math.PI/180,sl=180/Math.PI;function ia(){let n=Math.random()*4294967295|0,e=Math.random()*4294967295|0,t=Math.random()*4294967295|0,i=Math.random()*4294967295|0;return(Vt[n&255]+Vt[n>>8&255]+Vt[n>>16&255]+Vt[n>>24&255]+"-"+Vt[e&255]+Vt[e>>8&255]+"-"+Vt[e>>16&15|64]+Vt[e>>24&255]+"-"+Vt[t&63|128]+Vt[t>>8&255]+"-"+Vt[t>>16&255]+Vt[t>>24&255]+Vt[i&255]+Vt[i>>8&255]+Vt[i>>16&255]+Vt[i>>24&255]).toLowerCase()}function We(n,e,t){return Math.max(e,Math.min(t,n))}function nb(n,e){return(n%e+e)%e}function lh(n,e,t){return(1-t)*n+t*e}function To(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return n/4294967295;case Uint16Array:return n/65535;case Uint8Array:return n/255;case Int32Array:return Math.max(n/2147483647,-1);case Int16Array:return Math.max(n/32767,-1);case Int8Array:return Math.max(n/127,-1);default:throw new Error("Invalid component type.")}}function Jt(n,e){switch(e.constructor){case Float32Array:return n;case Uint32Array:return Math.round(n*4294967295);case Uint16Array:return Math.round(n*65535);case Uint8Array:return Math.round(n*255);case Int32Array:return Math.round(n*2147483647);case Int16Array:return Math.round(n*32767);case Int8Array:return Math.round(n*127);default:throw new Error("Invalid component type.")}}var tt=class n{static{n.prototype.isVector2=!0}constructor(e=0,t=0){this.x=e,this.y=t}get width(){return this.x}set width(e){this.x=e}get height(){return this.y}set height(e){this.y=e}set(e,t){return this.x=e,this.y=t,this}setScalar(e){return this.x=e,this.y=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y)}copy(e){return this.x=e.x,this.y=e.y,this}add(e){return this.x+=e.x,this.y+=e.y,this}addScalar(e){return this.x+=e,this.y+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this}subScalar(e){return this.x-=e,this.y-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this}multiply(e){return this.x*=e.x,this.y*=e.y,this}multiplyScalar(e){return this.x*=e,this.y*=e,this}divide(e){return this.x/=e.x,this.y/=e.y,this}divideScalar(e){return this.multiplyScalar(1/e)}applyMatrix3(e){let t=this.x,i=this.y,r=e.elements;return this.x=r[0]*t+r[3]*i+r[6],this.y=r[1]*t+r[4]*i+r[7],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this}clamp(e,t){return this.x=We(this.x,e.x,t.x),this.y=We(this.y,e.y,t.y),this}clampScalar(e,t){return this.x=We(this.x,e,t),this.y=We(this.y,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(We(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this}negate(){return this.x=-this.x,this.y=-this.y,this}dot(e){return this.x*e.x+this.y*e.y}cross(e){return this.x*e.y-this.y*e.x}lengthSq(){return this.x*this.x+this.y*this.y}length(){return Math.sqrt(this.x*this.x+this.y*this.y)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)}normalize(){return this.divideScalar(this.length()||1)}angle(){return Math.atan2(-this.y,-this.x)+Math.PI}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(We(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y;return t*t+i*i}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this}equals(e){return e.x===this.x&&e.y===this.y}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this}rotateAround(e,t){let i=Math.cos(t),r=Math.sin(t),s=this.x-e.x,o=this.y-e.y;return this.x=s*i-o*r+e.x,this.y=s*r+o*i+e.y,this}random(){return this.x=Math.random(),this.y=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y}},Kn=class{constructor(e=0,t=0,i=0,r=1){this.isQuaternion=!0,this._x=e,this._y=t,this._z=i,this._w=r}static slerpFlat(e,t,i,r,s,o,a){let c=i[r+0],l=i[r+1],u=i[r+2],f=i[r+3],d=s[o+0],h=s[o+1],v=s[o+2],M=s[o+3];if(f!==M||c!==d||l!==h||u!==v){let m=c*d+l*h+u*v+f*M;m<0&&(d=-d,h=-h,v=-v,M=-M,m=-m);let p=1-a;if(m<.9995){let S=Math.acos(m),T=Math.sin(S);p=Math.sin(p*S)/T,a=Math.sin(a*S)/T,c=c*p+d*a,l=l*p+h*a,u=u*p+v*a,f=f*p+M*a}else{c=c*p+d*a,l=l*p+h*a,u=u*p+v*a,f=f*p+M*a;let S=1/Math.sqrt(c*c+l*l+u*u+f*f);c*=S,l*=S,u*=S,f*=S}}e[t]=c,e[t+1]=l,e[t+2]=u,e[t+3]=f}static multiplyQuaternionsFlat(e,t,i,r,s,o){let a=i[r],c=i[r+1],l=i[r+2],u=i[r+3],f=s[o],d=s[o+1],h=s[o+2],v=s[o+3];return e[t]=a*v+u*f+c*h-l*d,e[t+1]=c*v+u*d+l*f-a*h,e[t+2]=l*v+u*h+a*d-c*f,e[t+3]=u*v-a*f-c*d-l*h,e}get x(){return this._x}set x(e){this._x=e,this._onChangeCallback()}get y(){return this._y}set y(e){this._y=e,this._onChangeCallback()}get z(){return this._z}set z(e){this._z=e,this._onChangeCallback()}get w(){return this._w}set w(e){this._w=e,this._onChangeCallback()}set(e,t,i,r){return this._x=e,this._y=t,this._z=i,this._w=r,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._w)}copy(e){return this._x=e.x,this._y=e.y,this._z=e.z,this._w=e.w,this._onChangeCallback(),this}setFromEuler(e,t=!0){let i=e._x,r=e._y,s=e._z,o=e._order,a=Math.cos,c=Math.sin,l=a(i/2),u=a(r/2),f=a(s/2),d=c(i/2),h=c(r/2),v=c(s/2);switch(o){case"XYZ":this._x=d*u*f+l*h*v,this._y=l*h*f-d*u*v,this._z=l*u*v+d*h*f,this._w=l*u*f-d*h*v;break;case"YXZ":this._x=d*u*f+l*h*v,this._y=l*h*f-d*u*v,this._z=l*u*v-d*h*f,this._w=l*u*f+d*h*v;break;case"ZXY":this._x=d*u*f-l*h*v,this._y=l*h*f+d*u*v,this._z=l*u*v+d*h*f,this._w=l*u*f-d*h*v;break;case"ZYX":this._x=d*u*f-l*h*v,this._y=l*h*f+d*u*v,this._z=l*u*v-d*h*f,this._w=l*u*f+d*h*v;break;case"YZX":this._x=d*u*f+l*h*v,this._y=l*h*f+d*u*v,this._z=l*u*v-d*h*f,this._w=l*u*f-d*h*v;break;case"XZY":this._x=d*u*f-l*h*v,this._y=l*h*f-d*u*v,this._z=l*u*v+d*h*f,this._w=l*u*f+d*h*v;break;default:be("Quaternion: .setFromEuler() encountered an unknown order: "+o)}return t===!0&&this._onChangeCallback(),this}setFromAxisAngle(e,t){let i=t/2,r=Math.sin(i);return this._x=e.x*r,this._y=e.y*r,this._z=e.z*r,this._w=Math.cos(i),this._onChangeCallback(),this}setFromRotationMatrix(e){let t=e.elements,i=t[0],r=t[4],s=t[8],o=t[1],a=t[5],c=t[9],l=t[2],u=t[6],f=t[10],d=i+a+f;if(d>0){let h=.5/Math.sqrt(d+1);this._w=.25/h,this._x=(u-c)*h,this._y=(s-l)*h,this._z=(o-r)*h}else if(i>a&&i>f){let h=2*Math.sqrt(1+i-a-f);this._w=(u-c)/h,this._x=.25*h,this._y=(r+o)/h,this._z=(s+l)/h}else if(a>f){let h=2*Math.sqrt(1+a-i-f);this._w=(s-l)/h,this._x=(r+o)/h,this._y=.25*h,this._z=(c+u)/h}else{let h=2*Math.sqrt(1+f-i-a);this._w=(o-r)/h,this._x=(s+l)/h,this._y=(c+u)/h,this._z=.25*h}return this._onChangeCallback(),this}setFromUnitVectors(e,t){let i=e.dot(t)+1;return i<1e-8?(i=0,Math.abs(e.x)>Math.abs(e.z)?(this._x=-e.y,this._y=e.x,this._z=0,this._w=i):(this._x=0,this._y=-e.z,this._z=e.y,this._w=i)):(this._x=e.y*t.z-e.z*t.y,this._y=e.z*t.x-e.x*t.z,this._z=e.x*t.y-e.y*t.x,this._w=i),this.normalize()}angleTo(e){return 2*Math.acos(Math.abs(We(this.dot(e),-1,1)))}rotateTowards(e,t){let i=this.angleTo(e);if(i===0)return this;let r=Math.min(1,t/i);return this.slerp(e,r),this}identity(){return this.set(0,0,0,1)}invert(){return this.conjugate()}conjugate(){return this._x*=-1,this._y*=-1,this._z*=-1,this._onChangeCallback(),this}dot(e){return this._x*e._x+this._y*e._y+this._z*e._z+this._w*e._w}lengthSq(){return this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w}length(){return Math.sqrt(this._x*this._x+this._y*this._y+this._z*this._z+this._w*this._w)}normalize(){let e=this.length();return e===0?(this._x=0,this._y=0,this._z=0,this._w=1):(e=1/e,this._x=this._x*e,this._y=this._y*e,this._z=this._z*e,this._w=this._w*e),this._onChangeCallback(),this}multiply(e){return this.multiplyQuaternions(this,e)}premultiply(e){return this.multiplyQuaternions(e,this)}multiplyQuaternions(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=t._x,c=t._y,l=t._z,u=t._w;return this._x=i*u+o*a+r*l-s*c,this._y=r*u+o*c+s*a-i*l,this._z=s*u+o*l+i*c-r*a,this._w=o*u-i*a-r*c-s*l,this._onChangeCallback(),this}slerp(e,t){let i=e._x,r=e._y,s=e._z,o=e._w,a=this.dot(e);a<0&&(i=-i,r=-r,s=-s,o=-o,a=-a);let c=1-t;if(a<.9995){let l=Math.acos(a),u=Math.sin(l);c=Math.sin(c*l)/u,t=Math.sin(t*l)/u,this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this._onChangeCallback()}else this._x=this._x*c+i*t,this._y=this._y*c+r*t,this._z=this._z*c+s*t,this._w=this._w*c+o*t,this.normalize();return this}slerpQuaternions(e,t,i){return this.copy(e).slerp(t,i)}random(){let e=2*Math.PI*Math.random(),t=2*Math.PI*Math.random(),i=Math.random(),r=Math.sqrt(1-i),s=Math.sqrt(i);return this.set(r*Math.sin(e),r*Math.cos(e),s*Math.sin(t),s*Math.cos(t))}equals(e){return e._x===this._x&&e._y===this._y&&e._z===this._z&&e._w===this._w}fromArray(e,t=0){return this._x=e[t],this._y=e[t+1],this._z=e[t+2],this._w=e[t+3],this._onChangeCallback(),this}toArray(e=[],t=0){return e[t]=this._x,e[t+1]=this._y,e[t+2]=this._z,e[t+3]=this._w,e}fromBufferAttribute(e,t){return this._x=e.getX(t),this._y=e.getY(t),this._z=e.getZ(t),this._w=e.getW(t),this._onChangeCallback(),this}toJSON(){return this.toArray()}_onChange(e){return this._onChangeCallback=e,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._w}},V=class n{static{n.prototype.isVector3=!0}constructor(e=0,t=0,i=0){this.x=e,this.y=t,this.z=i}set(e,t,i){return i===void 0&&(i=this.z),this.x=e,this.y=t,this.z=i,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this}multiplyVectors(e,t){return this.x=e.x*t.x,this.y=e.y*t.y,this.z=e.z*t.z,this}applyEuler(e){return this.applyQuaternion(py.setFromEuler(e))}applyAxisAngle(e,t){return this.applyQuaternion(py.setFromAxisAngle(e,t))}applyMatrix3(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[3]*i+s[6]*r,this.y=s[1]*t+s[4]*i+s[7]*r,this.z=s[2]*t+s[5]*i+s[8]*r,this}applyNormalMatrix(e){return this.applyMatrix3(e).normalize()}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=e.elements,o=1/(s[3]*t+s[7]*i+s[11]*r+s[15]);return this.x=(s[0]*t+s[4]*i+s[8]*r+s[12])*o,this.y=(s[1]*t+s[5]*i+s[9]*r+s[13])*o,this.z=(s[2]*t+s[6]*i+s[10]*r+s[14])*o,this}applyQuaternion(e){let t=this.x,i=this.y,r=this.z,s=e.x,o=e.y,a=e.z,c=e.w,l=2*(o*r-a*i),u=2*(a*t-s*r),f=2*(s*i-o*t);return this.x=t+c*l+o*f-a*u,this.y=i+c*u+a*l-s*f,this.z=r+c*f+s*u-o*l,this}project(e){return this.applyMatrix4(e.matrixWorldInverse).applyMatrix4(e.projectionMatrix)}unproject(e){return this.applyMatrix4(e.projectionMatrixInverse).applyMatrix4(e.matrixWorld)}transformDirection(e){let t=this.x,i=this.y,r=this.z,s=e.elements;return this.x=s[0]*t+s[4]*i+s[8]*r,this.y=s[1]*t+s[5]*i+s[9]*r,this.z=s[2]*t+s[6]*i+s[10]*r,this.normalize()}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this}divideScalar(e){return this.multiplyScalar(1/e)}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this}clamp(e,t){return this.x=We(this.x,e.x,t.x),this.y=We(this.y,e.y,t.y),this.z=We(this.z,e.z,t.z),this}clampScalar(e,t){return this.x=We(this.x,e,t),this.y=We(this.y,e,t),this.z=We(this.z,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(We(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this}cross(e){return this.crossVectors(this,e)}crossVectors(e,t){let i=e.x,r=e.y,s=e.z,o=t.x,a=t.y,c=t.z;return this.x=r*c-s*a,this.y=s*o-i*c,this.z=i*a-r*o,this}projectOnVector(e){let t=e.lengthSq();if(t===0)return this.set(0,0,0);let i=e.dot(this)/t;return this.copy(e).multiplyScalar(i)}projectOnPlane(e){return uh.copy(this).projectOnVector(e),this.sub(uh)}reflect(e){return this.sub(uh.copy(e).multiplyScalar(2*this.dot(e)))}angleTo(e){let t=Math.sqrt(this.lengthSq()*e.lengthSq());if(t===0)return Math.PI/2;let i=this.dot(e)/t;return Math.acos(We(i,-1,1))}distanceTo(e){return Math.sqrt(this.distanceToSquared(e))}distanceToSquared(e){let t=this.x-e.x,i=this.y-e.y,r=this.z-e.z;return t*t+i*i+r*r}manhattanDistanceTo(e){return Math.abs(this.x-e.x)+Math.abs(this.y-e.y)+Math.abs(this.z-e.z)}setFromSpherical(e){return this.setFromSphericalCoords(e.radius,e.phi,e.theta)}setFromSphericalCoords(e,t,i){let r=Math.sin(t)*e;return this.x=r*Math.sin(i),this.y=Math.cos(t)*e,this.z=r*Math.cos(i),this}setFromCylindrical(e){return this.setFromCylindricalCoords(e.radius,e.theta,e.y)}setFromCylindricalCoords(e,t,i){return this.x=e*Math.sin(t),this.y=i,this.z=e*Math.cos(t),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this}setFromMatrixScale(e){let t=this.setFromMatrixColumn(e,0).length(),i=this.setFromMatrixColumn(e,1).length(),r=this.setFromMatrixColumn(e,2).length();return this.x=t,this.y=i,this.z=r,this}setFromMatrixColumn(e,t){return this.fromArray(e.elements,t*4)}setFromMatrix3Column(e,t){return this.fromArray(e.elements,t*3)}setFromEuler(e){return this.x=e._x,this.y=e._y,this.z=e._z,this}setFromColor(e){return this.x=e.r,this.y=e.g,this.z=e.b,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this}randomDirection(){let e=Math.random()*Math.PI*2,t=Math.random()*2-1,i=Math.sqrt(1-t*t);return this.x=i*Math.cos(e),this.y=t,this.z=i*Math.sin(e),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z}},uh=new V,py=new Kn,Ae=class n{static{n.prototype.isMatrix3=!0}constructor(e,t,i,r,s,o,a,c,l){this.elements=[1,0,0,0,1,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l)}set(e,t,i,r,s,o,a,c,l){let u=this.elements;return u[0]=e,u[1]=r,u[2]=a,u[3]=t,u[4]=s,u[5]=c,u[6]=i,u[7]=o,u[8]=l,this}identity(){return this.set(1,0,0,0,1,0,0,0,1),this}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],this}extractBasis(e,t,i){return e.setFromMatrix3Column(this,0),t.setFromMatrix3Column(this,1),i.setFromMatrix3Column(this,2),this}setFromMatrix4(e){let t=e.elements;return this.set(t[0],t[4],t[8],t[1],t[5],t[9],t[2],t[6],t[10]),this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[3],c=i[6],l=i[1],u=i[4],f=i[7],d=i[2],h=i[5],v=i[8],M=r[0],m=r[3],p=r[6],S=r[1],T=r[4],E=r[7],D=r[2],b=r[5],A=r[8];return s[0]=o*M+a*S+c*D,s[3]=o*m+a*T+c*b,s[6]=o*p+a*E+c*A,s[1]=l*M+u*S+f*D,s[4]=l*m+u*T+f*b,s[7]=l*p+u*E+f*A,s[2]=d*M+h*S+v*D,s[5]=d*m+h*T+v*b,s[8]=d*p+h*E+v*A,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[3]*=e,t[6]*=e,t[1]*=e,t[4]*=e,t[7]*=e,t[2]*=e,t[5]*=e,t[8]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8];return t*o*u-t*a*l-i*s*u+i*a*c+r*s*l-r*o*c}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=u*o-a*l,d=a*c-u*s,h=l*s-o*c,v=t*f+i*d+r*h;if(v===0)return this.set(0,0,0,0,0,0,0,0,0);let M=1/v;return e[0]=f*M,e[1]=(r*l-u*i)*M,e[2]=(a*i-r*o)*M,e[3]=d*M,e[4]=(u*t-r*c)*M,e[5]=(r*s-a*t)*M,e[6]=h*M,e[7]=(i*c-l*t)*M,e[8]=(o*t-i*s)*M,this}transpose(){let e,t=this.elements;return e=t[1],t[1]=t[3],t[3]=e,e=t[2],t[2]=t[6],t[6]=e,e=t[5],t[5]=t[7],t[7]=e,this}getNormalMatrix(e){return this.setFromMatrix4(e).invert().transpose()}transposeIntoArray(e){let t=this.elements;return e[0]=t[0],e[1]=t[3],e[2]=t[6],e[3]=t[1],e[4]=t[4],e[5]=t[7],e[6]=t[2],e[7]=t[5],e[8]=t[8],this}setUvTransform(e,t,i,r,s,o,a){let c=Math.cos(s),l=Math.sin(s);return this.set(i*c,i*l,-i*(c*o+l*a)+o+e,-r*l,r*c,-r*(-l*o+c*a)+a+t,0,0,1),this}scale(e,t){return this.premultiply(dh.makeScale(e,t)),this}rotate(e){return this.premultiply(dh.makeRotation(-e)),this}translate(e,t){return this.premultiply(dh.makeTranslation(e,t)),this}makeTranslation(e,t){return e.isVector2?this.set(1,0,e.x,0,1,e.y,0,0,1):this.set(1,0,e,0,1,t,0,0,1),this}makeRotation(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,i,t,0,0,0,1),this}makeScale(e,t){return this.set(e,0,0,0,t,0,0,0,1),this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<9;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<9;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e}clone(){return new this.constructor().fromArray(this.elements)}},dh=new Ae,my=new Ae().set(.4123908,.3575843,.1804808,.212639,.7151687,.0721923,.0193308,.1191948,.9505322),gy=new Ae().set(3.2409699,-1.5373832,-.4986108,-.9692436,1.8759675,.0415551,.0556301,-.203977,1.0569715);function ib(){let n={enabled:!0,workingColorSpace:Ro,spaces:{},convert:function(r,s,o){return this.enabled===!1||s===o||!s||!o||(this.spaces[s].transfer===Qe&&(r.r=mi(r.r),r.g=mi(r.g),r.b=mi(r.b)),this.spaces[s].primaries!==this.spaces[o].primaries&&(r.applyMatrix3(this.spaces[s].toXYZ),r.applyMatrix3(this.spaces[o].fromXYZ)),this.spaces[o].transfer===Qe&&(r.r=ws(r.r),r.g=ws(r.g),r.b=ws(r.b))),r},workingToColorSpace:function(r,s){return this.convert(r,this.workingColorSpace,s)},colorSpaceToWorking:function(r,s){return this.convert(r,s,this.workingColorSpace)},getPrimaries:function(r){return this.spaces[r].primaries},getTransfer:function(r){return r===_i?No:this.spaces[r].transfer},getToneMappingMode:function(r){return this.spaces[r].outputColorSpaceConfig.toneMappingMode||"standard"},getLuminanceCoefficients:function(r,s=this.workingColorSpace){return r.fromArray(this.spaces[s].luminanceCoefficients)},define:function(r){Object.assign(this.spaces,r)},_getMatrix:function(r,s,o){return r.copy(this.spaces[s].toXYZ).multiply(this.spaces[o].fromXYZ)},_getDrawingBufferColorSpace:function(r){return this.spaces[r].outputColorSpaceConfig.drawingBufferColorSpace},_getUnpackColorSpace:function(r=this.workingColorSpace){return this.spaces[r].workingColorSpaceConfig.unpackColorSpace},fromWorkingColorSpace:function(r,s){return rl("ColorManagement: .fromWorkingColorSpace() has been renamed to .workingToColorSpace()."),n.workingToColorSpace(r,s)},toWorkingColorSpace:function(r,s){return rl("ColorManagement: .toWorkingColorSpace() has been renamed to .colorSpaceToWorking()."),n.colorSpaceToWorking(r,s)}},e=[.64,.33,.3,.6,.15,.06],t=[.2126,.7152,.0722],i=[.3127,.329];return n.define({[Ro]:{primaries:e,whitePoint:i,transfer:No,toXYZ:my,fromXYZ:gy,luminanceCoefficients:t,workingColorSpaceConfig:{unpackColorSpace:un},outputColorSpaceConfig:{drawingBufferColorSpace:un}},[un]:{primaries:e,whitePoint:i,transfer:Qe,toXYZ:my,fromXYZ:gy,luminanceCoefficients:t,outputColorSpaceConfig:{drawingBufferColorSpace:un}}}),n}var ze=ib();function mi(n){return n<.04045?n*.0773993808:Math.pow(n*.9478672986+.0521327014,2.4)}function ws(n){return n<.0031308?n*12.92:1.055*Math.pow(n,.41666)-.055}var ps,ol=class{static getDataURL(e,t="image/png"){if(/^data:/i.test(e.src)||typeof HTMLCanvasElement>"u")return e.src;let i;if(e instanceof HTMLCanvasElement)i=e;else{ps===void 0&&(ps=Lo("canvas")),ps.width=e.width,ps.height=e.height;let r=ps.getContext("2d");e instanceof ImageData?r.putImageData(e,0,0):r.drawImage(e,0,0,e.width,e.height),i=ps}return i.toDataURL(t)}static sRGBToLinear(e){if(typeof HTMLImageElement<"u"&&e instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&e instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&e instanceof ImageBitmap){let t=Lo("canvas");t.width=e.width,t.height=e.height;let i=t.getContext("2d");i.drawImage(e,0,0,e.width,e.height);let r=i.getImageData(0,0,e.width,e.height),s=r.data;for(let o=0;o<s.length;o++)s[o]=mi(s[o]/255)*255;return i.putImageData(r,0,0),t}else if(e.data){let t=e.data.slice(0);for(let i=0;i<t.length;i++)t instanceof Uint8Array||t instanceof Uint8ClampedArray?t[i]=Math.floor(mi(t[i]/255)*255):t[i]=mi(t[i]);return{data:t,width:e.width,height:e.height}}else return be("ImageUtils.sRGBToLinear(): Unsupported image type. No color space conversion applied."),e}},rb=0,Cs=class{constructor(e=null){this.isSource=!0,Object.defineProperty(this,"id",{value:rb++}),this.uuid=ia(),this.data=e,this.dataReady=!0,this.version=0}getSize(e){let t=this.data;return typeof HTMLVideoElement<"u"&&t instanceof HTMLVideoElement?e.set(t.videoWidth,t.videoHeight,0):typeof VideoFrame<"u"&&t instanceof VideoFrame?e.set(t.displayWidth,t.displayHeight,0):t!==null?e.set(t.width,t.height,t.depth||0):e.set(0,0,0),e}set needsUpdate(e){e===!0&&this.version++}toJSON(e){let t=e===void 0||typeof e=="string";if(!t&&e.images[this.uuid]!==void 0)return e.images[this.uuid];let i={uuid:this.uuid,url:""},r=this.data;if(r!==null){let s;if(Array.isArray(r)){s=[];for(let o=0,a=r.length;o<a;o++)r[o].isDataTexture?s.push(fh(r[o].image)):s.push(fh(r[o]))}else s=fh(r);i.url=s}return t||(e.images[this.uuid]=i),i}};function fh(n){return typeof HTMLImageElement<"u"&&n instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&n instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&n instanceof ImageBitmap?ol.getDataURL(n):n.data?{data:Array.from(n.data),width:n.width,height:n.height,type:n.data.constructor.name}:(be("Texture: Unable to serialize Texture."),{})}var sb=0,hh=new V,xi=(()=>{class n extends Jn{constructor(t=n.DEFAULT_IMAGE,i=n.DEFAULT_MAPPING,r=Yn,s=Yn,o=Ot,a=$i,c=bn,l=pn,u=n.DEFAULT_ANISOTROPY,f=_i){super(),this.isTexture=!0,Object.defineProperty(this,"id",{value:sb++}),this.uuid=ia(),this.name="",this.source=new Cs(t),this.mipmaps=[],this.mapping=i,this.channel=0,this.wrapS=r,this.wrapT=s,this.magFilter=o,this.minFilter=a,this.anisotropy=u,this.format=c,this.internalFormat=null,this.type=l,this.offset=new tt(0,0),this.repeat=new tt(1,1),this.center=new tt(0,0),this.rotation=0,this.matrixAutoUpdate=!0,this.matrix=new Ae,this.generateMipmaps=!0,this.premultiplyAlpha=!1,this.flipY=!0,this.unpackAlignment=4,this.colorSpace=f,this.userData={},this.updateRanges=[],this.version=0,this.onUpdate=null,this.renderTarget=null,this.isRenderTargetTexture=!1,this.isArrayTexture=!!(t&&t.depth&&t.depth>1),this.pmremVersion=0,this.normalized=!1}get width(){return this.source.getSize(hh).x}get height(){return this.source.getSize(hh).y}get depth(){return this.source.getSize(hh).z}get image(){return this.source.data}set image(t){this.source.data=t}updateMatrix(){this.matrix.setUvTransform(this.offset.x,this.offset.y,this.repeat.x,this.repeat.y,this.rotation,this.center.x,this.center.y)}addUpdateRange(t,i){this.updateRanges.push({start:t,count:i})}clearUpdateRanges(){this.updateRanges.length=0}clone(){return new this.constructor().copy(this)}copy(t){return this.name=t.name,this.source=t.source,this.mipmaps=t.mipmaps.slice(0),this.mapping=t.mapping,this.channel=t.channel,this.wrapS=t.wrapS,this.wrapT=t.wrapT,this.magFilter=t.magFilter,this.minFilter=t.minFilter,this.anisotropy=t.anisotropy,this.format=t.format,this.internalFormat=t.internalFormat,this.type=t.type,this.normalized=t.normalized,this.offset.copy(t.offset),this.repeat.copy(t.repeat),this.center.copy(t.center),this.rotation=t.rotation,this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrix.copy(t.matrix),this.generateMipmaps=t.generateMipmaps,this.premultiplyAlpha=t.premultiplyAlpha,this.flipY=t.flipY,this.unpackAlignment=t.unpackAlignment,this.colorSpace=t.colorSpace,this.renderTarget=t.renderTarget,this.isRenderTargetTexture=t.isRenderTargetTexture,this.isArrayTexture=t.isArrayTexture,this.userData=JSON.parse(JSON.stringify(t.userData)),this.needsUpdate=!0,this}setValues(t){for(let i in t){let r=t[i];if(r===void 0){be(`Texture.setValues(): parameter '${i}' has value of undefined.`);continue}let s=this[i];if(s===void 0){be(`Texture.setValues(): property '${i}' does not exist.`);continue}s&&r&&s.isVector2&&r.isVector2||s&&r&&s.isVector3&&r.isVector3||s&&r&&s.isMatrix3&&r.isMatrix3?s.copy(r):this[i]=r}}toJSON(t){let i=t===void 0||typeof t=="string";if(!i&&t.textures[this.uuid]!==void 0)return t.textures[this.uuid];let r={metadata:{version:4.7,type:"Texture",generator:"Texture.toJSON"},uuid:this.uuid,name:this.name,image:this.source.toJSON(t).uuid,mapping:this.mapping,channel:this.channel,repeat:[this.repeat.x,this.repeat.y],offset:[this.offset.x,this.offset.y],center:[this.center.x,this.center.y],rotation:this.rotation,wrap:[this.wrapS,this.wrapT],format:this.format,internalFormat:this.internalFormat,type:this.type,normalized:this.normalized,colorSpace:this.colorSpace,minFilter:this.minFilter,magFilter:this.magFilter,anisotropy:this.anisotropy,flipY:this.flipY,generateMipmaps:this.generateMipmaps,premultiplyAlpha:this.premultiplyAlpha,unpackAlignment:this.unpackAlignment};return Object.keys(this.userData).length>0&&(r.userData=this.userData),i||(t.textures[this.uuid]=r),r}dispose(){this.dispatchEvent({type:"dispose"})}transformUv(t){if(this.mapping!==Rh)return t;if(t.applyMatrix3(this.matrix),t.x<0||t.x>1)switch(this.wrapS){case tl:t.x=t.x-Math.floor(t.x);break;case Yn:t.x=t.x<0?0:1;break;case nl:Math.abs(Math.floor(t.x)%2)===1?t.x=Math.ceil(t.x)-t.x:t.x=t.x-Math.floor(t.x);break}if(t.y<0||t.y>1)switch(this.wrapT){case tl:t.y=t.y-Math.floor(t.y);break;case Yn:t.y=t.y<0?0:1;break;case nl:Math.abs(Math.floor(t.y)%2)===1?t.y=Math.ceil(t.y)-t.y:t.y=t.y-Math.floor(t.y);break}return this.flipY&&(t.y=1-t.y),t}set needsUpdate(t){t===!0&&(this.version++,this.source.needsUpdate=!0)}set needsPMREMUpdate(t){t===!0&&this.pmremVersion++}}return n.DEFAULT_IMAGE=null,n.DEFAULT_MAPPING=Rh,n.DEFAULT_ANISOTROPY=1,n})(),_t=class n{static{n.prototype.isVector4=!0}constructor(e=0,t=0,i=0,r=1){this.x=e,this.y=t,this.z=i,this.w=r}get width(){return this.z}set width(e){this.z=e}get height(){return this.w}set height(e){this.w=e}set(e,t,i,r){return this.x=e,this.y=t,this.z=i,this.w=r,this}setScalar(e){return this.x=e,this.y=e,this.z=e,this.w=e,this}setX(e){return this.x=e,this}setY(e){return this.y=e,this}setZ(e){return this.z=e,this}setW(e){return this.w=e,this}setComponent(e,t){switch(e){case 0:this.x=t;break;case 1:this.y=t;break;case 2:this.z=t;break;case 3:this.w=t;break;default:throw new Error("index is out of range: "+e)}return this}getComponent(e){switch(e){case 0:return this.x;case 1:return this.y;case 2:return this.z;case 3:return this.w;default:throw new Error("index is out of range: "+e)}}clone(){return new this.constructor(this.x,this.y,this.z,this.w)}copy(e){return this.x=e.x,this.y=e.y,this.z=e.z,this.w=e.w!==void 0?e.w:1,this}add(e){return this.x+=e.x,this.y+=e.y,this.z+=e.z,this.w+=e.w,this}addScalar(e){return this.x+=e,this.y+=e,this.z+=e,this.w+=e,this}addVectors(e,t){return this.x=e.x+t.x,this.y=e.y+t.y,this.z=e.z+t.z,this.w=e.w+t.w,this}addScaledVector(e,t){return this.x+=e.x*t,this.y+=e.y*t,this.z+=e.z*t,this.w+=e.w*t,this}sub(e){return this.x-=e.x,this.y-=e.y,this.z-=e.z,this.w-=e.w,this}subScalar(e){return this.x-=e,this.y-=e,this.z-=e,this.w-=e,this}subVectors(e,t){return this.x=e.x-t.x,this.y=e.y-t.y,this.z=e.z-t.z,this.w=e.w-t.w,this}multiply(e){return this.x*=e.x,this.y*=e.y,this.z*=e.z,this.w*=e.w,this}multiplyScalar(e){return this.x*=e,this.y*=e,this.z*=e,this.w*=e,this}applyMatrix4(e){let t=this.x,i=this.y,r=this.z,s=this.w,o=e.elements;return this.x=o[0]*t+o[4]*i+o[8]*r+o[12]*s,this.y=o[1]*t+o[5]*i+o[9]*r+o[13]*s,this.z=o[2]*t+o[6]*i+o[10]*r+o[14]*s,this.w=o[3]*t+o[7]*i+o[11]*r+o[15]*s,this}divide(e){return this.x/=e.x,this.y/=e.y,this.z/=e.z,this.w/=e.w,this}divideScalar(e){return this.multiplyScalar(1/e)}setAxisAngleFromQuaternion(e){this.w=2*Math.acos(e.w);let t=Math.sqrt(1-e.w*e.w);return t<1e-4?(this.x=1,this.y=0,this.z=0):(this.x=e.x/t,this.y=e.y/t,this.z=e.z/t),this}setAxisAngleFromRotationMatrix(e){let t,i,r,s,c=e.elements,l=c[0],u=c[4],f=c[8],d=c[1],h=c[5],v=c[9],M=c[2],m=c[6],p=c[10];if(Math.abs(u-d)<.01&&Math.abs(f-M)<.01&&Math.abs(v-m)<.01){if(Math.abs(u+d)<.1&&Math.abs(f+M)<.1&&Math.abs(v+m)<.1&&Math.abs(l+h+p-3)<.1)return this.set(1,0,0,0),this;t=Math.PI;let T=(l+1)/2,E=(h+1)/2,D=(p+1)/2,b=(u+d)/4,A=(f+M)/4,y=(v+m)/4;return T>E&&T>D?T<.01?(i=0,r=.707106781,s=.707106781):(i=Math.sqrt(T),r=b/i,s=A/i):E>D?E<.01?(i=.707106781,r=0,s=.707106781):(r=Math.sqrt(E),i=b/r,s=y/r):D<.01?(i=.707106781,r=.707106781,s=0):(s=Math.sqrt(D),i=A/s,r=y/s),this.set(i,r,s,t),this}let S=Math.sqrt((m-v)*(m-v)+(f-M)*(f-M)+(d-u)*(d-u));return Math.abs(S)<.001&&(S=1),this.x=(m-v)/S,this.y=(f-M)/S,this.z=(d-u)/S,this.w=Math.acos((l+h+p-1)/2),this}setFromMatrixPosition(e){let t=e.elements;return this.x=t[12],this.y=t[13],this.z=t[14],this.w=t[15],this}min(e){return this.x=Math.min(this.x,e.x),this.y=Math.min(this.y,e.y),this.z=Math.min(this.z,e.z),this.w=Math.min(this.w,e.w),this}max(e){return this.x=Math.max(this.x,e.x),this.y=Math.max(this.y,e.y),this.z=Math.max(this.z,e.z),this.w=Math.max(this.w,e.w),this}clamp(e,t){return this.x=We(this.x,e.x,t.x),this.y=We(this.y,e.y,t.y),this.z=We(this.z,e.z,t.z),this.w=We(this.w,e.w,t.w),this}clampScalar(e,t){return this.x=We(this.x,e,t),this.y=We(this.y,e,t),this.z=We(this.z,e,t),this.w=We(this.w,e,t),this}clampLength(e,t){let i=this.length();return this.divideScalar(i||1).multiplyScalar(We(i,e,t))}floor(){return this.x=Math.floor(this.x),this.y=Math.floor(this.y),this.z=Math.floor(this.z),this.w=Math.floor(this.w),this}ceil(){return this.x=Math.ceil(this.x),this.y=Math.ceil(this.y),this.z=Math.ceil(this.z),this.w=Math.ceil(this.w),this}round(){return this.x=Math.round(this.x),this.y=Math.round(this.y),this.z=Math.round(this.z),this.w=Math.round(this.w),this}roundToZero(){return this.x=Math.trunc(this.x),this.y=Math.trunc(this.y),this.z=Math.trunc(this.z),this.w=Math.trunc(this.w),this}negate(){return this.x=-this.x,this.y=-this.y,this.z=-this.z,this.w=-this.w,this}dot(e){return this.x*e.x+this.y*e.y+this.z*e.z+this.w*e.w}lengthSq(){return this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w}length(){return Math.sqrt(this.x*this.x+this.y*this.y+this.z*this.z+this.w*this.w)}manhattanLength(){return Math.abs(this.x)+Math.abs(this.y)+Math.abs(this.z)+Math.abs(this.w)}normalize(){return this.divideScalar(this.length()||1)}setLength(e){return this.normalize().multiplyScalar(e)}lerp(e,t){return this.x+=(e.x-this.x)*t,this.y+=(e.y-this.y)*t,this.z+=(e.z-this.z)*t,this.w+=(e.w-this.w)*t,this}lerpVectors(e,t,i){return this.x=e.x+(t.x-e.x)*i,this.y=e.y+(t.y-e.y)*i,this.z=e.z+(t.z-e.z)*i,this.w=e.w+(t.w-e.w)*i,this}equals(e){return e.x===this.x&&e.y===this.y&&e.z===this.z&&e.w===this.w}fromArray(e,t=0){return this.x=e[t],this.y=e[t+1],this.z=e[t+2],this.w=e[t+3],this}toArray(e=[],t=0){return e[t]=this.x,e[t+1]=this.y,e[t+2]=this.z,e[t+3]=this.w,e}fromBufferAttribute(e,t){return this.x=e.getX(t),this.y=e.getY(t),this.z=e.getZ(t),this.w=e.getW(t),this}random(){return this.x=Math.random(),this.y=Math.random(),this.z=Math.random(),this.w=Math.random(),this}*[Symbol.iterator](){yield this.x,yield this.y,yield this.z,yield this.w}},al=class extends Jn{constructor(e=1,t=1,i={}){super(),i=Object.assign({generateMipmaps:!1,internalFormat:null,minFilter:Ot,depthBuffer:!0,stencilBuffer:!1,resolveDepthBuffer:!0,resolveStencilBuffer:!0,depthTexture:null,samples:0,count:1,depth:1,multiview:!1},i),this.isRenderTarget=!0,this.width=e,this.height=t,this.depth=i.depth,this.scissor=new _t(0,0,e,t),this.scissorTest=!1,this.viewport=new _t(0,0,e,t),this.textures=[];let r={width:e,height:t,depth:i.depth},s=new xi(r),o=i.count;for(let a=0;a<o;a++)this.textures[a]=s.clone(),this.textures[a].isRenderTargetTexture=!0,this.textures[a].renderTarget=this;this._setTextureOptions(i),this.depthBuffer=i.depthBuffer,this.stencilBuffer=i.stencilBuffer,this.resolveDepthBuffer=i.resolveDepthBuffer,this.resolveStencilBuffer=i.resolveStencilBuffer,this._depthTexture=null,this.depthTexture=i.depthTexture,this.samples=i.samples,this.multiview=i.multiview}_setTextureOptions(e={}){let t={minFilter:Ot,generateMipmaps:!1,flipY:!1,internalFormat:null};e.mapping!==void 0&&(t.mapping=e.mapping),e.wrapS!==void 0&&(t.wrapS=e.wrapS),e.wrapT!==void 0&&(t.wrapT=e.wrapT),e.wrapR!==void 0&&(t.wrapR=e.wrapR),e.magFilter!==void 0&&(t.magFilter=e.magFilter),e.minFilter!==void 0&&(t.minFilter=e.minFilter),e.format!==void 0&&(t.format=e.format),e.type!==void 0&&(t.type=e.type),e.anisotropy!==void 0&&(t.anisotropy=e.anisotropy),e.colorSpace!==void 0&&(t.colorSpace=e.colorSpace),e.flipY!==void 0&&(t.flipY=e.flipY),e.generateMipmaps!==void 0&&(t.generateMipmaps=e.generateMipmaps),e.internalFormat!==void 0&&(t.internalFormat=e.internalFormat);for(let i=0;i<this.textures.length;i++)this.textures[i].setValues(t)}get texture(){return this.textures[0]}set texture(e){this.textures[0]=e}set depthTexture(e){this._depthTexture!==null&&(this._depthTexture.renderTarget=null),e!==null&&(e.renderTarget=this),this._depthTexture=e}get depthTexture(){return this._depthTexture}setSize(e,t,i=1){if(this.width!==e||this.height!==t||this.depth!==i){this.width=e,this.height=t,this.depth=i;for(let r=0,s=this.textures.length;r<s;r++)this.textures[r].image.width=e,this.textures[r].image.height=t,this.textures[r].image.depth=i,this.textures[r].isData3DTexture!==!0&&(this.textures[r].isArrayTexture=this.textures[r].image.depth>1);this.dispose()}this.viewport.set(0,0,e,t),this.scissor.set(0,0,e,t)}clone(){return new this.constructor().copy(this)}copy(e){this.width=e.width,this.height=e.height,this.depth=e.depth,this.scissor.copy(e.scissor),this.scissorTest=e.scissorTest,this.viewport.copy(e.viewport),this.textures.length=0;for(let t=0,i=e.textures.length;t<i;t++){this.textures[t]=e.textures[t].clone(),this.textures[t].isRenderTargetTexture=!0,this.textures[t].renderTarget=this;let r=Object.assign({},e.textures[t].image);this.textures[t].source=new Cs(r)}return this.depthBuffer=e.depthBuffer,this.stencilBuffer=e.stencilBuffer,this.resolveDepthBuffer=e.resolveDepthBuffer,this.resolveStencilBuffer=e.resolveStencilBuffer,e.depthTexture!==null&&(this.depthTexture=e.depthTexture.clone()),this.samples=e.samples,this.multiview=e.multiview,this}dispose(){this.dispatchEvent({type:"dispose"})}},dn=class extends al{constructor(e=1,t=1,i={}){super(e,t,i),this.isWebGLRenderTarget=!0}},Oo=class extends xi{constructor(e=null,t=1,i=1,r=1){super(null),this.isDataArrayTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Pt,this.minFilter=Pt,this.wrapR=Yn,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1,this.layerUpdates=new Set}addLayerUpdate(e){this.layerUpdates.add(e)}clearLayerUpdates(){this.layerUpdates.clear()}};var cl=class extends xi{constructor(e=null,t=1,i=1,r=1){super(null),this.isData3DTexture=!0,this.image={data:e,width:t,height:i,depth:r},this.magFilter=Pt,this.minFilter=Pt,this.wrapR=Yn,this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Tt=class n{static{n.prototype.isMatrix4=!0}constructor(e,t,i,r,s,o,a,c,l,u,f,d,h,v,M,m){this.elements=[1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1],e!==void 0&&this.set(e,t,i,r,s,o,a,c,l,u,f,d,h,v,M,m)}set(e,t,i,r,s,o,a,c,l,u,f,d,h,v,M,m){let p=this.elements;return p[0]=e,p[4]=t,p[8]=i,p[12]=r,p[1]=s,p[5]=o,p[9]=a,p[13]=c,p[2]=l,p[6]=u,p[10]=f,p[14]=d,p[3]=h,p[7]=v,p[11]=M,p[15]=m,this}identity(){return this.set(1,0,0,0,0,1,0,0,0,0,1,0,0,0,0,1),this}clone(){return new n().fromArray(this.elements)}copy(e){let t=this.elements,i=e.elements;return t[0]=i[0],t[1]=i[1],t[2]=i[2],t[3]=i[3],t[4]=i[4],t[5]=i[5],t[6]=i[6],t[7]=i[7],t[8]=i[8],t[9]=i[9],t[10]=i[10],t[11]=i[11],t[12]=i[12],t[13]=i[13],t[14]=i[14],t[15]=i[15],this}copyPosition(e){let t=this.elements,i=e.elements;return t[12]=i[12],t[13]=i[13],t[14]=i[14],this}setFromMatrix3(e){let t=e.elements;return this.set(t[0],t[3],t[6],0,t[1],t[4],t[7],0,t[2],t[5],t[8],0,0,0,0,1),this}extractBasis(e,t,i){return this.determinant()===0?(e.set(1,0,0),t.set(0,1,0),i.set(0,0,1),this):(e.setFromMatrixColumn(this,0),t.setFromMatrixColumn(this,1),i.setFromMatrixColumn(this,2),this)}makeBasis(e,t,i){return this.set(e.x,t.x,i.x,0,e.y,t.y,i.y,0,e.z,t.z,i.z,0,0,0,0,1),this}extractRotation(e){if(e.determinant()===0)return this.identity();let t=this.elements,i=e.elements,r=1/ms.setFromMatrixColumn(e,0).length(),s=1/ms.setFromMatrixColumn(e,1).length(),o=1/ms.setFromMatrixColumn(e,2).length();return t[0]=i[0]*r,t[1]=i[1]*r,t[2]=i[2]*r,t[3]=0,t[4]=i[4]*s,t[5]=i[5]*s,t[6]=i[6]*s,t[7]=0,t[8]=i[8]*o,t[9]=i[9]*o,t[10]=i[10]*o,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromEuler(e){let t=this.elements,i=e.x,r=e.y,s=e.z,o=Math.cos(i),a=Math.sin(i),c=Math.cos(r),l=Math.sin(r),u=Math.cos(s),f=Math.sin(s);if(e.order==="XYZ"){let d=o*u,h=o*f,v=a*u,M=a*f;t[0]=c*u,t[4]=-c*f,t[8]=l,t[1]=h+v*l,t[5]=d-M*l,t[9]=-a*c,t[2]=M-d*l,t[6]=v+h*l,t[10]=o*c}else if(e.order==="YXZ"){let d=c*u,h=c*f,v=l*u,M=l*f;t[0]=d+M*a,t[4]=v*a-h,t[8]=o*l,t[1]=o*f,t[5]=o*u,t[9]=-a,t[2]=h*a-v,t[6]=M+d*a,t[10]=o*c}else if(e.order==="ZXY"){let d=c*u,h=c*f,v=l*u,M=l*f;t[0]=d-M*a,t[4]=-o*f,t[8]=v+h*a,t[1]=h+v*a,t[5]=o*u,t[9]=M-d*a,t[2]=-o*l,t[6]=a,t[10]=o*c}else if(e.order==="ZYX"){let d=o*u,h=o*f,v=a*u,M=a*f;t[0]=c*u,t[4]=v*l-h,t[8]=d*l+M,t[1]=c*f,t[5]=M*l+d,t[9]=h*l-v,t[2]=-l,t[6]=a*c,t[10]=o*c}else if(e.order==="YZX"){let d=o*c,h=o*l,v=a*c,M=a*l;t[0]=c*u,t[4]=M-d*f,t[8]=v*f+h,t[1]=f,t[5]=o*u,t[9]=-a*u,t[2]=-l*u,t[6]=h*f+v,t[10]=d-M*f}else if(e.order==="XZY"){let d=o*c,h=o*l,v=a*c,M=a*l;t[0]=c*u,t[4]=-f,t[8]=l*u,t[1]=d*f+M,t[5]=o*u,t[9]=h*f-v,t[2]=v*f-h,t[6]=a*u,t[10]=M*f+d}return t[3]=0,t[7]=0,t[11]=0,t[12]=0,t[13]=0,t[14]=0,t[15]=1,this}makeRotationFromQuaternion(e){return this.compose(ob,e,ab)}lookAt(e,t,i){let r=this.elements;return cn.subVectors(e,t),cn.lengthSq()===0&&(cn.z=1),cn.normalize(),Pi.crossVectors(i,cn),Pi.lengthSq()===0&&(Math.abs(i.z)===1?cn.x+=1e-4:cn.z+=1e-4,cn.normalize(),Pi.crossVectors(i,cn)),Pi.normalize(),Tc.crossVectors(cn,Pi),r[0]=Pi.x,r[4]=Tc.x,r[8]=cn.x,r[1]=Pi.y,r[5]=Tc.y,r[9]=cn.y,r[2]=Pi.z,r[6]=Tc.z,r[10]=cn.z,this}multiply(e){return this.multiplyMatrices(this,e)}premultiply(e){return this.multiplyMatrices(e,this)}multiplyMatrices(e,t){let i=e.elements,r=t.elements,s=this.elements,o=i[0],a=i[4],c=i[8],l=i[12],u=i[1],f=i[5],d=i[9],h=i[13],v=i[2],M=i[6],m=i[10],p=i[14],S=i[3],T=i[7],E=i[11],D=i[15],b=r[0],A=r[4],y=r[8],w=r[12],L=r[1],I=r[5],k=r[9],W=r[13],X=r[2],P=r[6],z=r[10],F=r[14],K=r[3],Q=r[7],le=r[11],ye=r[15];return s[0]=o*b+a*L+c*X+l*K,s[4]=o*A+a*I+c*P+l*Q,s[8]=o*y+a*k+c*z+l*le,s[12]=o*w+a*W+c*F+l*ye,s[1]=u*b+f*L+d*X+h*K,s[5]=u*A+f*I+d*P+h*Q,s[9]=u*y+f*k+d*z+h*le,s[13]=u*w+f*W+d*F+h*ye,s[2]=v*b+M*L+m*X+p*K,s[6]=v*A+M*I+m*P+p*Q,s[10]=v*y+M*k+m*z+p*le,s[14]=v*w+M*W+m*F+p*ye,s[3]=S*b+T*L+E*X+D*K,s[7]=S*A+T*I+E*P+D*Q,s[11]=S*y+T*k+E*z+D*le,s[15]=S*w+T*W+E*F+D*ye,this}multiplyScalar(e){let t=this.elements;return t[0]*=e,t[4]*=e,t[8]*=e,t[12]*=e,t[1]*=e,t[5]*=e,t[9]*=e,t[13]*=e,t[2]*=e,t[6]*=e,t[10]*=e,t[14]*=e,t[3]*=e,t[7]*=e,t[11]*=e,t[15]*=e,this}determinant(){let e=this.elements,t=e[0],i=e[4],r=e[8],s=e[12],o=e[1],a=e[5],c=e[9],l=e[13],u=e[2],f=e[6],d=e[10],h=e[14],v=e[3],M=e[7],m=e[11],p=e[15],S=c*h-l*d,T=a*h-l*f,E=a*d-c*f,D=o*h-l*u,b=o*d-c*u,A=o*f-a*u;return t*(M*S-m*T+p*E)-i*(v*S-m*D+p*b)+r*(v*T-M*D+p*A)-s*(v*E-M*b+m*A)}transpose(){let e=this.elements,t;return t=e[1],e[1]=e[4],e[4]=t,t=e[2],e[2]=e[8],e[8]=t,t=e[6],e[6]=e[9],e[9]=t,t=e[3],e[3]=e[12],e[12]=t,t=e[7],e[7]=e[13],e[13]=t,t=e[11],e[11]=e[14],e[14]=t,this}setPosition(e,t,i){let r=this.elements;return e.isVector3?(r[12]=e.x,r[13]=e.y,r[14]=e.z):(r[12]=e,r[13]=t,r[14]=i),this}invert(){let e=this.elements,t=e[0],i=e[1],r=e[2],s=e[3],o=e[4],a=e[5],c=e[6],l=e[7],u=e[8],f=e[9],d=e[10],h=e[11],v=e[12],M=e[13],m=e[14],p=e[15],S=t*a-i*o,T=t*c-r*o,E=t*l-s*o,D=i*c-r*a,b=i*l-s*a,A=r*l-s*c,y=u*M-f*v,w=u*m-d*v,L=u*p-h*v,I=f*m-d*M,k=f*p-h*M,W=d*p-h*m,X=S*W-T*k+E*I+D*L-b*w+A*y;if(X===0)return this.set(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);let P=1/X;return e[0]=(a*W-c*k+l*I)*P,e[1]=(r*k-i*W-s*I)*P,e[2]=(M*A-m*b+p*D)*P,e[3]=(d*b-f*A-h*D)*P,e[4]=(c*L-o*W-l*w)*P,e[5]=(t*W-r*L+s*w)*P,e[6]=(m*E-v*A-p*T)*P,e[7]=(u*A-d*E+h*T)*P,e[8]=(o*k-a*L+l*y)*P,e[9]=(i*L-t*k-s*y)*P,e[10]=(v*b-M*E+p*S)*P,e[11]=(f*E-u*b-h*S)*P,e[12]=(a*w-o*I-c*y)*P,e[13]=(t*I-i*w+r*y)*P,e[14]=(M*T-v*D-m*S)*P,e[15]=(u*D-f*T+d*S)*P,this}scale(e){let t=this.elements,i=e.x,r=e.y,s=e.z;return t[0]*=i,t[4]*=r,t[8]*=s,t[1]*=i,t[5]*=r,t[9]*=s,t[2]*=i,t[6]*=r,t[10]*=s,t[3]*=i,t[7]*=r,t[11]*=s,this}getMaxScaleOnAxis(){let e=this.elements,t=e[0]*e[0]+e[1]*e[1]+e[2]*e[2],i=e[4]*e[4]+e[5]*e[5]+e[6]*e[6],r=e[8]*e[8]+e[9]*e[9]+e[10]*e[10];return Math.sqrt(Math.max(t,i,r))}makeTranslation(e,t,i){return e.isVector3?this.set(1,0,0,e.x,0,1,0,e.y,0,0,1,e.z,0,0,0,1):this.set(1,0,0,e,0,1,0,t,0,0,1,i,0,0,0,1),this}makeRotationX(e){let t=Math.cos(e),i=Math.sin(e);return this.set(1,0,0,0,0,t,-i,0,0,i,t,0,0,0,0,1),this}makeRotationY(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,0,i,0,0,1,0,0,-i,0,t,0,0,0,0,1),this}makeRotationZ(e){let t=Math.cos(e),i=Math.sin(e);return this.set(t,-i,0,0,i,t,0,0,0,0,1,0,0,0,0,1),this}makeRotationAxis(e,t){let i=Math.cos(t),r=Math.sin(t),s=1-i,o=e.x,a=e.y,c=e.z,l=s*o,u=s*a;return this.set(l*o+i,l*a-r*c,l*c+r*a,0,l*a+r*c,u*a+i,u*c-r*o,0,l*c-r*a,u*c+r*o,s*c*c+i,0,0,0,0,1),this}makeScale(e,t,i){return this.set(e,0,0,0,0,t,0,0,0,0,i,0,0,0,0,1),this}makeShear(e,t,i,r,s,o){return this.set(1,i,s,0,e,1,o,0,t,r,1,0,0,0,0,1),this}compose(e,t,i){let r=this.elements,s=t._x,o=t._y,a=t._z,c=t._w,l=s+s,u=o+o,f=a+a,d=s*l,h=s*u,v=s*f,M=o*u,m=o*f,p=a*f,S=c*l,T=c*u,E=c*f,D=i.x,b=i.y,A=i.z;return r[0]=(1-(M+p))*D,r[1]=(h+E)*D,r[2]=(v-T)*D,r[3]=0,r[4]=(h-E)*b,r[5]=(1-(d+p))*b,r[6]=(m+S)*b,r[7]=0,r[8]=(v+T)*A,r[9]=(m-S)*A,r[10]=(1-(d+M))*A,r[11]=0,r[12]=e.x,r[13]=e.y,r[14]=e.z,r[15]=1,this}decompose(e,t,i){let r=this.elements;e.x=r[12],e.y=r[13],e.z=r[14];let s=this.determinant();if(s===0)return i.set(1,1,1),t.identity(),this;let o=ms.set(r[0],r[1],r[2]).length(),a=ms.set(r[4],r[5],r[6]).length(),c=ms.set(r[8],r[9],r[10]).length();s<0&&(o=-o),Nn.copy(this);let l=1/o,u=1/a,f=1/c;return Nn.elements[0]*=l,Nn.elements[1]*=l,Nn.elements[2]*=l,Nn.elements[4]*=u,Nn.elements[5]*=u,Nn.elements[6]*=u,Nn.elements[8]*=f,Nn.elements[9]*=f,Nn.elements[10]*=f,t.setFromRotationMatrix(Nn),i.x=o,i.y=a,i.z=c,this}makePerspective(e,t,i,r,s,o,a=On,c=!1){let l=this.elements,u=2*s/(t-e),f=2*s/(i-r),d=(t+e)/(t-e),h=(i+r)/(i-r),v,M;if(c)v=s/(o-s),M=o*s/(o-s);else if(a===On)v=-(o+s)/(o-s),M=-2*o*s/(o-s);else if(a===Po)v=-o/(o-s),M=-o*s/(o-s);else throw new Error("THREE.Matrix4.makePerspective(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=d,l[12]=0,l[1]=0,l[5]=f,l[9]=h,l[13]=0,l[2]=0,l[6]=0,l[10]=v,l[14]=M,l[3]=0,l[7]=0,l[11]=-1,l[15]=0,this}makeOrthographic(e,t,i,r,s,o,a=On,c=!1){let l=this.elements,u=2/(t-e),f=2/(i-r),d=-(t+e)/(t-e),h=-(i+r)/(i-r),v,M;if(c)v=1/(o-s),M=o/(o-s);else if(a===On)v=-2/(o-s),M=-(o+s)/(o-s);else if(a===Po)v=-1/(o-s),M=-s/(o-s);else throw new Error("THREE.Matrix4.makeOrthographic(): Invalid coordinate system: "+a);return l[0]=u,l[4]=0,l[8]=0,l[12]=d,l[1]=0,l[5]=f,l[9]=0,l[13]=h,l[2]=0,l[6]=0,l[10]=v,l[14]=M,l[3]=0,l[7]=0,l[11]=0,l[15]=1,this}equals(e){let t=this.elements,i=e.elements;for(let r=0;r<16;r++)if(t[r]!==i[r])return!1;return!0}fromArray(e,t=0){for(let i=0;i<16;i++)this.elements[i]=e[i+t];return this}toArray(e=[],t=0){let i=this.elements;return e[t]=i[0],e[t+1]=i[1],e[t+2]=i[2],e[t+3]=i[3],e[t+4]=i[4],e[t+5]=i[5],e[t+6]=i[6],e[t+7]=i[7],e[t+8]=i[8],e[t+9]=i[9],e[t+10]=i[10],e[t+11]=i[11],e[t+12]=i[12],e[t+13]=i[13],e[t+14]=i[14],e[t+15]=i[15],e}},ms=new V,Nn=new Tt,ob=new V(0,0,0),ab=new V(1,1,1),Pi=new V,Tc=new V,cn=new V,vy=new Tt,yy=new Kn,Fo=(()=>{class n{constructor(t=0,i=0,r=0,s=n.DEFAULT_ORDER){this.isEuler=!0,this._x=t,this._y=i,this._z=r,this._order=s}get x(){return this._x}set x(t){this._x=t,this._onChangeCallback()}get y(){return this._y}set y(t){this._y=t,this._onChangeCallback()}get z(){return this._z}set z(t){this._z=t,this._onChangeCallback()}get order(){return this._order}set order(t){this._order=t,this._onChangeCallback()}set(t,i,r,s=this._order){return this._x=t,this._y=i,this._z=r,this._order=s,this._onChangeCallback(),this}clone(){return new this.constructor(this._x,this._y,this._z,this._order)}copy(t){return this._x=t._x,this._y=t._y,this._z=t._z,this._order=t._order,this._onChangeCallback(),this}setFromRotationMatrix(t,i=this._order,r=!0){let s=t.elements,o=s[0],a=s[4],c=s[8],l=s[1],u=s[5],f=s[9],d=s[2],h=s[6],v=s[10];switch(i){case"XYZ":this._y=Math.asin(We(c,-1,1)),Math.abs(c)<.9999999?(this._x=Math.atan2(-f,v),this._z=Math.atan2(-a,o)):(this._x=Math.atan2(h,u),this._z=0);break;case"YXZ":this._x=Math.asin(-We(f,-1,1)),Math.abs(f)<.9999999?(this._y=Math.atan2(c,v),this._z=Math.atan2(l,u)):(this._y=Math.atan2(-d,o),this._z=0);break;case"ZXY":this._x=Math.asin(We(h,-1,1)),Math.abs(h)<.9999999?(this._y=Math.atan2(-d,v),this._z=Math.atan2(-a,u)):(this._y=0,this._z=Math.atan2(l,o));break;case"ZYX":this._y=Math.asin(-We(d,-1,1)),Math.abs(d)<.9999999?(this._x=Math.atan2(h,v),this._z=Math.atan2(l,o)):(this._x=0,this._z=Math.atan2(-a,u));break;case"YZX":this._z=Math.asin(We(l,-1,1)),Math.abs(l)<.9999999?(this._x=Math.atan2(-f,u),this._y=Math.atan2(-d,o)):(this._x=0,this._y=Math.atan2(c,v));break;case"XZY":this._z=Math.asin(-We(a,-1,1)),Math.abs(a)<.9999999?(this._x=Math.atan2(h,u),this._y=Math.atan2(c,o)):(this._x=Math.atan2(-f,v),this._y=0);break;default:be("Euler: .setFromRotationMatrix() encountered an unknown order: "+i)}return this._order=i,r===!0&&this._onChangeCallback(),this}setFromQuaternion(t,i,r){return vy.makeRotationFromQuaternion(t),this.setFromRotationMatrix(vy,i,r)}setFromVector3(t,i=this._order){return this.set(t.x,t.y,t.z,i)}reorder(t){return yy.setFromEuler(this),this.setFromQuaternion(yy,t)}equals(t){return t._x===this._x&&t._y===this._y&&t._z===this._z&&t._order===this._order}fromArray(t){return this._x=t[0],this._y=t[1],this._z=t[2],t[3]!==void 0&&(this._order=t[3]),this._onChangeCallback(),this}toArray(t=[],i=0){return t[i]=this._x,t[i+1]=this._y,t[i+2]=this._z,t[i+3]=this._order,t}_onChange(t){return this._onChangeCallback=t,this}_onChangeCallback(){}*[Symbol.iterator](){yield this._x,yield this._y,yield this._z,yield this._order}}return n.DEFAULT_ORDER="XYZ",n})(),ko=class{constructor(){this.mask=1}set(e){this.mask=(1<<e|0)>>>0}enable(e){this.mask|=1<<e|0}enableAll(){this.mask=-1}toggle(e){this.mask^=1<<e|0}disable(e){this.mask&=~(1<<e|0)}disableAll(){this.mask=0}test(e){return(this.mask&e.mask)!==0}isEnabled(e){return(this.mask&(1<<e|0))!==0}},cb=0,_y=new V,gs=new Kn,ui=new Tt,wc=new V,wo=new V,lb=new V,ub=new Kn,xy=new V(1,0,0),My=new V(0,1,0),Sy=new V(0,0,1),Ey={type:"added"},db={type:"removed"},vs={type:"childadded",child:null},ph={type:"childremoved",child:null},Rr=(()=>{class n extends Jn{constructor(){super(),this.isObject3D=!0,Object.defineProperty(this,"id",{value:cb++}),this.uuid=ia(),this.name="",this.type="Object3D",this.parent=null,this.children=[],this.up=n.DEFAULT_UP.clone();let t=new V,i=new Fo,r=new Kn,s=new V(1,1,1);function o(){r.setFromEuler(i,!1)}function a(){i.setFromQuaternion(r,void 0,!1)}i._onChange(o),r._onChange(a),Object.defineProperties(this,{position:{configurable:!0,enumerable:!0,value:t},rotation:{configurable:!0,enumerable:!0,value:i},quaternion:{configurable:!0,enumerable:!0,value:r},scale:{configurable:!0,enumerable:!0,value:s},modelViewMatrix:{value:new Tt},normalMatrix:{value:new Ae}}),this.matrix=new Tt,this.matrixWorld=new Tt,this.matrixAutoUpdate=n.DEFAULT_MATRIX_AUTO_UPDATE,this.matrixWorldAutoUpdate=n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE,this.matrixWorldNeedsUpdate=!1,this.layers=new ko,this.visible=!0,this.castShadow=!1,this.receiveShadow=!1,this.frustumCulled=!0,this.renderOrder=0,this.animations=[],this.customDepthMaterial=void 0,this.customDistanceMaterial=void 0,this.static=!1,this.userData={},this.pivot=null}onBeforeShadow(){}onAfterShadow(){}onBeforeRender(){}onAfterRender(){}applyMatrix4(t){this.matrixAutoUpdate&&this.updateMatrix(),this.matrix.premultiply(t),this.matrix.decompose(this.position,this.quaternion,this.scale)}applyQuaternion(t){return this.quaternion.premultiply(t),this}setRotationFromAxisAngle(t,i){this.quaternion.setFromAxisAngle(t,i)}setRotationFromEuler(t){this.quaternion.setFromEuler(t,!0)}setRotationFromMatrix(t){this.quaternion.setFromRotationMatrix(t)}setRotationFromQuaternion(t){this.quaternion.copy(t)}rotateOnAxis(t,i){return gs.setFromAxisAngle(t,i),this.quaternion.multiply(gs),this}rotateOnWorldAxis(t,i){return gs.setFromAxisAngle(t,i),this.quaternion.premultiply(gs),this}rotateX(t){return this.rotateOnAxis(xy,t)}rotateY(t){return this.rotateOnAxis(My,t)}rotateZ(t){return this.rotateOnAxis(Sy,t)}translateOnAxis(t,i){return _y.copy(t).applyQuaternion(this.quaternion),this.position.add(_y.multiplyScalar(i)),this}translateX(t){return this.translateOnAxis(xy,t)}translateY(t){return this.translateOnAxis(My,t)}translateZ(t){return this.translateOnAxis(Sy,t)}localToWorld(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(this.matrixWorld)}worldToLocal(t){return this.updateWorldMatrix(!0,!1),t.applyMatrix4(ui.copy(this.matrixWorld).invert())}lookAt(t,i,r){t.isVector3?wc.copy(t):wc.set(t,i,r);let s=this.parent;this.updateWorldMatrix(!0,!1),wo.setFromMatrixPosition(this.matrixWorld),this.isCamera||this.isLight?ui.lookAt(wo,wc,this.up):ui.lookAt(wc,wo,this.up),this.quaternion.setFromRotationMatrix(ui),s&&(ui.extractRotation(s.matrixWorld),gs.setFromRotationMatrix(ui),this.quaternion.premultiply(gs.invert()))}add(t){if(arguments.length>1){for(let i=0;i<arguments.length;i++)this.add(arguments[i]);return this}return t===this?(we("Object3D.add: object can't be added as a child of itself.",t),this):(t&&t.isObject3D?(t.removeFromParent(),t.parent=this,this.children.push(t),t.dispatchEvent(Ey),vs.child=t,this.dispatchEvent(vs),vs.child=null):we("Object3D.add: object not an instance of THREE.Object3D.",t),this)}remove(t){if(arguments.length>1){for(let r=0;r<arguments.length;r++)this.remove(arguments[r]);return this}let i=this.children.indexOf(t);return i!==-1&&(t.parent=null,this.children.splice(i,1),t.dispatchEvent(db),ph.child=t,this.dispatchEvent(ph),ph.child=null),this}removeFromParent(){let t=this.parent;return t!==null&&t.remove(this),this}clear(){return this.remove(...this.children)}attach(t){return this.updateWorldMatrix(!0,!1),ui.copy(this.matrixWorld).invert(),t.parent!==null&&(t.parent.updateWorldMatrix(!0,!1),ui.multiply(t.parent.matrixWorld)),t.applyMatrix4(ui),t.removeFromParent(),t.parent=this,this.children.push(t),t.updateWorldMatrix(!1,!0),t.dispatchEvent(Ey),vs.child=t,this.dispatchEvent(vs),vs.child=null,this}getObjectById(t){return this.getObjectByProperty("id",t)}getObjectByName(t){return this.getObjectByProperty("name",t)}getObjectByProperty(t,i){if(this[t]===i)return this;for(let r=0,s=this.children.length;r<s;r++){let a=this.children[r].getObjectByProperty(t,i);if(a!==void 0)return a}}getObjectsByProperty(t,i,r=[]){this[t]===i&&r.push(this);let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].getObjectsByProperty(t,i,r);return r}getWorldPosition(t){return this.updateWorldMatrix(!0,!1),t.setFromMatrixPosition(this.matrixWorld)}getWorldQuaternion(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(wo,t,lb),t}getWorldScale(t){return this.updateWorldMatrix(!0,!1),this.matrixWorld.decompose(wo,ub,t),t}getWorldDirection(t){this.updateWorldMatrix(!0,!1);let i=this.matrixWorld.elements;return t.set(i[8],i[9],i[10]).normalize()}raycast(){}traverse(t){t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverse(t)}traverseVisible(t){if(this.visible===!1)return;t(this);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].traverseVisible(t)}traverseAncestors(t){let i=this.parent;i!==null&&(t(i),i.traverseAncestors(t))}updateMatrix(){this.matrix.compose(this.position,this.quaternion,this.scale);let t=this.pivot;if(t!==null){let i=t.x,r=t.y,s=t.z,o=this.matrix.elements;o[12]+=i-o[0]*i-o[4]*r-o[8]*s,o[13]+=r-o[1]*i-o[5]*r-o[9]*s,o[14]+=s-o[2]*i-o[6]*r-o[10]*s}this.matrixWorldNeedsUpdate=!0}updateMatrixWorld(t){this.matrixAutoUpdate&&this.updateMatrix(),(this.matrixWorldNeedsUpdate||t)&&(this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),this.matrixWorldNeedsUpdate=!1,t=!0);let i=this.children;for(let r=0,s=i.length;r<s;r++)i[r].updateMatrixWorld(t)}updateWorldMatrix(t,i){let r=this.parent;if(t===!0&&r!==null&&r.updateWorldMatrix(!0,!1),this.matrixAutoUpdate&&this.updateMatrix(),this.matrixWorldAutoUpdate===!0&&(this.parent===null?this.matrixWorld.copy(this.matrix):this.matrixWorld.multiplyMatrices(this.parent.matrixWorld,this.matrix)),i===!0){let s=this.children;for(let o=0,a=s.length;o<a;o++)s[o].updateWorldMatrix(!1,!0)}}toJSON(t){let i=t===void 0||typeof t=="string",r={};i&&(t={geometries:{},materials:{},textures:{},images:{},shapes:{},skeletons:{},animations:{},nodes:{}},r.metadata={version:4.7,type:"Object",generator:"Object3D.toJSON"});let s={};s.uuid=this.uuid,s.type=this.type,this.name!==""&&(s.name=this.name),this.castShadow===!0&&(s.castShadow=!0),this.receiveShadow===!0&&(s.receiveShadow=!0),this.visible===!1&&(s.visible=!1),this.frustumCulled===!1&&(s.frustumCulled=!1),this.renderOrder!==0&&(s.renderOrder=this.renderOrder),this.static!==!1&&(s.static=this.static),Object.keys(this.userData).length>0&&(s.userData=this.userData),s.layers=this.layers.mask,s.matrix=this.matrix.toArray(),s.up=this.up.toArray(),this.pivot!==null&&(s.pivot=this.pivot.toArray()),this.matrixAutoUpdate===!1&&(s.matrixAutoUpdate=!1),this.morphTargetDictionary!==void 0&&(s.morphTargetDictionary=Object.assign({},this.morphTargetDictionary)),this.morphTargetInfluences!==void 0&&(s.morphTargetInfluences=this.morphTargetInfluences.slice()),this.isInstancedMesh&&(s.type="InstancedMesh",s.count=this.count,s.instanceMatrix=this.instanceMatrix.toJSON(),this.instanceColor!==null&&(s.instanceColor=this.instanceColor.toJSON())),this.isBatchedMesh&&(s.type="BatchedMesh",s.perObjectFrustumCulled=this.perObjectFrustumCulled,s.sortObjects=this.sortObjects,s.drawRanges=this._drawRanges,s.reservedRanges=this._reservedRanges,s.geometryInfo=this._geometryInfo.map(c=>Xt(Lt({},c),{boundingBox:c.boundingBox?c.boundingBox.toJSON():void 0,boundingSphere:c.boundingSphere?c.boundingSphere.toJSON():void 0})),s.instanceInfo=this._instanceInfo.map(c=>Lt({},c)),s.availableInstanceIds=this._availableInstanceIds.slice(),s.availableGeometryIds=this._availableGeometryIds.slice(),s.nextIndexStart=this._nextIndexStart,s.nextVertexStart=this._nextVertexStart,s.geometryCount=this._geometryCount,s.maxInstanceCount=this._maxInstanceCount,s.maxVertexCount=this._maxVertexCount,s.maxIndexCount=this._maxIndexCount,s.geometryInitialized=this._geometryInitialized,s.matricesTexture=this._matricesTexture.toJSON(t),s.indirectTexture=this._indirectTexture.toJSON(t),this._colorsTexture!==null&&(s.colorsTexture=this._colorsTexture.toJSON(t)),this.boundingSphere!==null&&(s.boundingSphere=this.boundingSphere.toJSON()),this.boundingBox!==null&&(s.boundingBox=this.boundingBox.toJSON()));function o(c,l){return c[l.uuid]===void 0&&(c[l.uuid]=l.toJSON(t)),l.uuid}if(this.isScene)this.background&&(this.background.isColor?s.background=this.background.toJSON():this.background.isTexture&&(s.background=this.background.toJSON(t).uuid)),this.environment&&this.environment.isTexture&&this.environment.isRenderTargetTexture!==!0&&(s.environment=this.environment.toJSON(t).uuid);else if(this.isMesh||this.isLine||this.isPoints){s.geometry=o(t.geometries,this.geometry);let c=this.geometry.parameters;if(c!==void 0&&c.shapes!==void 0){let l=c.shapes;if(Array.isArray(l))for(let u=0,f=l.length;u<f;u++){let d=l[u];o(t.shapes,d)}else o(t.shapes,l)}}if(this.isSkinnedMesh&&(s.bindMode=this.bindMode,s.bindMatrix=this.bindMatrix.toArray(),this.skeleton!==void 0&&(o(t.skeletons,this.skeleton),s.skeleton=this.skeleton.uuid)),this.material!==void 0)if(Array.isArray(this.material)){let c=[];for(let l=0,u=this.material.length;l<u;l++)c.push(o(t.materials,this.material[l]));s.material=c}else s.material=o(t.materials,this.material);if(this.children.length>0){s.children=[];for(let c=0;c<this.children.length;c++)s.children.push(this.children[c].toJSON(t).object)}if(this.animations.length>0){s.animations=[];for(let c=0;c<this.animations.length;c++){let l=this.animations[c];s.animations.push(o(t.animations,l))}}if(i){let c=a(t.geometries),l=a(t.materials),u=a(t.textures),f=a(t.images),d=a(t.shapes),h=a(t.skeletons),v=a(t.animations),M=a(t.nodes);c.length>0&&(r.geometries=c),l.length>0&&(r.materials=l),u.length>0&&(r.textures=u),f.length>0&&(r.images=f),d.length>0&&(r.shapes=d),h.length>0&&(r.skeletons=h),v.length>0&&(r.animations=v),M.length>0&&(r.nodes=M)}return r.object=s,r;function a(c){let l=[];for(let u in c){let f=c[u];delete f.metadata,l.push(f)}return l}}clone(t){return new this.constructor().copy(this,t)}copy(t,i=!0){if(this.name=t.name,this.up.copy(t.up),this.position.copy(t.position),this.rotation.order=t.rotation.order,this.quaternion.copy(t.quaternion),this.scale.copy(t.scale),this.pivot=t.pivot!==null?t.pivot.clone():null,this.matrix.copy(t.matrix),this.matrixWorld.copy(t.matrixWorld),this.matrixAutoUpdate=t.matrixAutoUpdate,this.matrixWorldAutoUpdate=t.matrixWorldAutoUpdate,this.matrixWorldNeedsUpdate=t.matrixWorldNeedsUpdate,this.layers.mask=t.layers.mask,this.visible=t.visible,this.castShadow=t.castShadow,this.receiveShadow=t.receiveShadow,this.frustumCulled=t.frustumCulled,this.renderOrder=t.renderOrder,this.static=t.static,this.animations=t.animations.slice(),this.userData=JSON.parse(JSON.stringify(t.userData)),i===!0)for(let r=0;r<t.children.length;r++){let s=t.children[r];this.add(s.clone())}return this}}return n.DEFAULT_UP=new V(0,1,0),n.DEFAULT_MATRIX_AUTO_UPDATE=!0,n.DEFAULT_MATRIX_WORLD_AUTO_UPDATE=!0,n})(),wr=class extends Rr{constructor(){super(),this.isGroup=!0,this.type="Group"}},fb={type:"move"},As=class{constructor(){this._targetRay=null,this._grip=null,this._hand=null}getHandSpace(){return this._hand===null&&(this._hand=new wr,this._hand.matrixAutoUpdate=!1,this._hand.visible=!1,this._hand.joints={},this._hand.inputState={pinching:!1}),this._hand}getTargetRaySpace(){return this._targetRay===null&&(this._targetRay=new wr,this._targetRay.matrixAutoUpdate=!1,this._targetRay.visible=!1,this._targetRay.hasLinearVelocity=!1,this._targetRay.linearVelocity=new V,this._targetRay.hasAngularVelocity=!1,this._targetRay.angularVelocity=new V),this._targetRay}getGripSpace(){return this._grip===null&&(this._grip=new wr,this._grip.matrixAutoUpdate=!1,this._grip.visible=!1,this._grip.hasLinearVelocity=!1,this._grip.linearVelocity=new V,this._grip.hasAngularVelocity=!1,this._grip.angularVelocity=new V,this._grip.eventsEnabled=!1),this._grip}dispatchEvent(e){return this._targetRay!==null&&this._targetRay.dispatchEvent(e),this._grip!==null&&this._grip.dispatchEvent(e),this._hand!==null&&this._hand.dispatchEvent(e),this}connect(e){if(e&&e.hand){let t=this._hand;if(t)for(let i of e.hand.values())this._getHandJoint(t,i)}return this.dispatchEvent({type:"connected",data:e}),this}disconnect(e){return this.dispatchEvent({type:"disconnected",data:e}),this._targetRay!==null&&(this._targetRay.visible=!1),this._grip!==null&&(this._grip.visible=!1),this._hand!==null&&(this._hand.visible=!1),this}update(e,t,i){let r=null,s=null,o=null,a=this._targetRay,c=this._grip,l=this._hand;if(e&&t.session.visibilityState!=="visible-blurred"){if(l&&e.hand){o=!0;for(let M of e.hand.values()){let m=t.getJointPose(M,i),p=this._getHandJoint(l,M);m!==null&&(p.matrix.fromArray(m.transform.matrix),p.matrix.decompose(p.position,p.rotation,p.scale),p.matrixWorldNeedsUpdate=!0,p.jointRadius=m.radius),p.visible=m!==null}let u=l.joints["index-finger-tip"],f=l.joints["thumb-tip"],d=u.position.distanceTo(f.position),h=.02,v=.005;l.inputState.pinching&&d>h+v?(l.inputState.pinching=!1,this.dispatchEvent({type:"pinchend",handedness:e.handedness,target:this})):!l.inputState.pinching&&d<=h-v&&(l.inputState.pinching=!0,this.dispatchEvent({type:"pinchstart",handedness:e.handedness,target:this}))}else c!==null&&e.gripSpace&&(s=t.getPose(e.gripSpace,i),s!==null&&(c.matrix.fromArray(s.transform.matrix),c.matrix.decompose(c.position,c.rotation,c.scale),c.matrixWorldNeedsUpdate=!0,s.linearVelocity?(c.hasLinearVelocity=!0,c.linearVelocity.copy(s.linearVelocity)):c.hasLinearVelocity=!1,s.angularVelocity?(c.hasAngularVelocity=!0,c.angularVelocity.copy(s.angularVelocity)):c.hasAngularVelocity=!1,c.eventsEnabled&&c.dispatchEvent({type:"gripUpdated",data:e,target:this})));a!==null&&(r=t.getPose(e.targetRaySpace,i),r===null&&s!==null&&(r=s),r!==null&&(a.matrix.fromArray(r.transform.matrix),a.matrix.decompose(a.position,a.rotation,a.scale),a.matrixWorldNeedsUpdate=!0,r.linearVelocity?(a.hasLinearVelocity=!0,a.linearVelocity.copy(r.linearVelocity)):a.hasLinearVelocity=!1,r.angularVelocity?(a.hasAngularVelocity=!0,a.angularVelocity.copy(r.angularVelocity)):a.hasAngularVelocity=!1,this.dispatchEvent(fb)))}return a!==null&&(a.visible=r!==null),c!==null&&(c.visible=s!==null),l!==null&&(l.visible=o!==null),this}_getHandJoint(e,t){if(e.joints[t.jointName]===void 0){let i=new wr;i.matrixAutoUpdate=!1,i.visible=!1,e.joints[t.jointName]=i,e.add(i)}return e.joints[t.jointName]}},h_={aliceblue:15792383,antiquewhite:16444375,aqua:65535,aquamarine:8388564,azure:15794175,beige:16119260,bisque:16770244,black:0,blanchedalmond:16772045,blue:255,blueviolet:9055202,brown:10824234,burlywood:14596231,cadetblue:6266528,chartreuse:8388352,chocolate:13789470,coral:16744272,cornflowerblue:6591981,cornsilk:16775388,crimson:14423100,cyan:65535,darkblue:139,darkcyan:35723,darkgoldenrod:12092939,darkgray:11119017,darkgreen:25600,darkgrey:11119017,darkkhaki:12433259,darkmagenta:9109643,darkolivegreen:5597999,darkorange:16747520,darkorchid:10040012,darkred:9109504,darksalmon:15308410,darkseagreen:9419919,darkslateblue:4734347,darkslategray:3100495,darkslategrey:3100495,darkturquoise:52945,darkviolet:9699539,deeppink:16716947,deepskyblue:49151,dimgray:6908265,dimgrey:6908265,dodgerblue:2003199,firebrick:11674146,floralwhite:16775920,forestgreen:2263842,fuchsia:16711935,gainsboro:14474460,ghostwhite:16316671,gold:16766720,goldenrod:14329120,gray:8421504,green:32768,greenyellow:11403055,grey:8421504,honeydew:15794160,hotpink:16738740,indianred:13458524,indigo:4915330,ivory:16777200,khaki:15787660,lavender:15132410,lavenderblush:16773365,lawngreen:8190976,lemonchiffon:16775885,lightblue:11393254,lightcoral:15761536,lightcyan:14745599,lightgoldenrodyellow:16448210,lightgray:13882323,lightgreen:9498256,lightgrey:13882323,lightpink:16758465,lightsalmon:16752762,lightseagreen:2142890,lightskyblue:8900346,lightslategray:7833753,lightslategrey:7833753,lightsteelblue:11584734,lightyellow:16777184,lime:65280,limegreen:3329330,linen:16445670,magenta:16711935,maroon:8388608,mediumaquamarine:6737322,mediumblue:205,mediumorchid:12211667,mediumpurple:9662683,mediumseagreen:3978097,mediumslateblue:8087790,mediumspringgreen:64154,mediumturquoise:4772300,mediumvioletred:13047173,midnightblue:1644912,mintcream:16121850,mistyrose:16770273,moccasin:16770229,navajowhite:16768685,navy:128,oldlace:16643558,olive:8421376,olivedrab:7048739,orange:16753920,orangered:16729344,orchid:14315734,palegoldenrod:15657130,palegreen:10025880,paleturquoise:11529966,palevioletred:14381203,papayawhip:16773077,peachpuff:16767673,peru:13468991,pink:16761035,plum:14524637,powderblue:11591910,purple:8388736,rebeccapurple:6697881,red:16711680,rosybrown:12357519,royalblue:4286945,saddlebrown:9127187,salmon:16416882,sandybrown:16032864,seagreen:3050327,seashell:16774638,sienna:10506797,silver:12632256,skyblue:8900331,slateblue:6970061,slategray:7372944,slategrey:7372944,snow:16775930,springgreen:65407,steelblue:4620980,tan:13808780,teal:32896,thistle:14204888,tomato:16737095,turquoise:4251856,violet:15631086,wheat:16113331,white:16777215,whitesmoke:16119285,yellow:16776960,yellowgreen:10145074},Li={h:0,s:0,l:0},Ic={h:0,s:0,l:0};function mh(n,e,t){return t<0&&(t+=1),t>1&&(t-=1),t<1/6?n+(e-n)*6*t:t<1/2?e:t<2/3?n+(e-n)*6*(2/3-t):n}var Ze=class{constructor(e,t,i){return this.isColor=!0,this.r=1,this.g=1,this.b=1,this.set(e,t,i)}set(e,t,i){if(t===void 0&&i===void 0){let r=e;r&&r.isColor?this.copy(r):typeof r=="number"?this.setHex(r):typeof r=="string"&&this.setStyle(r)}else this.setRGB(e,t,i);return this}setScalar(e){return this.r=e,this.g=e,this.b=e,this}setHex(e,t=un){return e=Math.floor(e),this.r=(e>>16&255)/255,this.g=(e>>8&255)/255,this.b=(e&255)/255,ze.colorSpaceToWorking(this,t),this}setRGB(e,t,i,r=ze.workingColorSpace){return this.r=e,this.g=t,this.b=i,ze.colorSpaceToWorking(this,r),this}setHSL(e,t,i,r=ze.workingColorSpace){if(e=nb(e,1),t=We(t,0,1),i=We(i,0,1),t===0)this.r=this.g=this.b=i;else{let s=i<=.5?i*(1+t):i+t-i*t,o=2*i-s;this.r=mh(o,s,e+1/3),this.g=mh(o,s,e),this.b=mh(o,s,e-1/3)}return ze.colorSpaceToWorking(this,r),this}setStyle(e,t=un){function i(s){s!==void 0&&parseFloat(s)<1&&be("Color: Alpha component of "+e+" will be ignored.")}let r;if(r=/^(\w+)\(([^\)]*)\)/.exec(e)){let s,o=r[1],a=r[2];switch(o){case"rgb":case"rgba":if(s=/^\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(255,parseInt(s[1],10))/255,Math.min(255,parseInt(s[2],10))/255,Math.min(255,parseInt(s[3],10))/255,t);if(s=/^\s*(\d+)\%\s*,\s*(\d+)\%\s*,\s*(\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setRGB(Math.min(100,parseInt(s[1],10))/100,Math.min(100,parseInt(s[2],10))/100,Math.min(100,parseInt(s[3],10))/100,t);break;case"hsl":case"hsla":if(s=/^\s*(\d*\.?\d+)\s*,\s*(\d*\.?\d+)\%\s*,\s*(\d*\.?\d+)\%\s*(?:,\s*(\d*\.?\d+)\s*)?$/.exec(a))return i(s[4]),this.setHSL(parseFloat(s[1])/360,parseFloat(s[2])/100,parseFloat(s[3])/100,t);break;default:be("Color: Unknown color model "+e)}}else if(r=/^\#([A-Fa-f\d]+)$/.exec(e)){let s=r[1],o=s.length;if(o===3)return this.setRGB(parseInt(s.charAt(0),16)/15,parseInt(s.charAt(1),16)/15,parseInt(s.charAt(2),16)/15,t);if(o===6)return this.setHex(parseInt(s,16),t);be("Color: Invalid hex color "+e)}else if(e&&e.length>0)return this.setColorName(e,t);return this}setColorName(e,t=un){let i=h_[e.toLowerCase()];return i!==void 0?this.setHex(i,t):be("Color: Unknown color "+e),this}clone(){return new this.constructor(this.r,this.g,this.b)}copy(e){return this.r=e.r,this.g=e.g,this.b=e.b,this}copySRGBToLinear(e){return this.r=mi(e.r),this.g=mi(e.g),this.b=mi(e.b),this}copyLinearToSRGB(e){return this.r=ws(e.r),this.g=ws(e.g),this.b=ws(e.b),this}convertSRGBToLinear(){return this.copySRGBToLinear(this),this}convertLinearToSRGB(){return this.copyLinearToSRGB(this),this}getHex(e=un){return ze.workingToColorSpace(Ht.copy(this),e),Math.round(We(Ht.r*255,0,255))*65536+Math.round(We(Ht.g*255,0,255))*256+Math.round(We(Ht.b*255,0,255))}getHexString(e=un){return("000000"+this.getHex(e).toString(16)).slice(-6)}getHSL(e,t=ze.workingColorSpace){ze.workingToColorSpace(Ht.copy(this),t);let i=Ht.r,r=Ht.g,s=Ht.b,o=Math.max(i,r,s),a=Math.min(i,r,s),c,l,u=(a+o)/2;if(a===o)c=0,l=0;else{let f=o-a;switch(l=u<=.5?f/(o+a):f/(2-o-a),o){case i:c=(r-s)/f+(r<s?6:0);break;case r:c=(s-i)/f+2;break;case s:c=(i-r)/f+4;break}c/=6}return e.h=c,e.s=l,e.l=u,e}getRGB(e,t=ze.workingColorSpace){return ze.workingToColorSpace(Ht.copy(this),t),e.r=Ht.r,e.g=Ht.g,e.b=Ht.b,e}getStyle(e=un){ze.workingToColorSpace(Ht.copy(this),e);let t=Ht.r,i=Ht.g,r=Ht.b;return e!==un?`color(${e} ${t.toFixed(3)} ${i.toFixed(3)} ${r.toFixed(3)})`:`rgb(${Math.round(t*255)},${Math.round(i*255)},${Math.round(r*255)})`}offsetHSL(e,t,i){return this.getHSL(Li),this.setHSL(Li.h+e,Li.s+t,Li.l+i)}add(e){return this.r+=e.r,this.g+=e.g,this.b+=e.b,this}addColors(e,t){return this.r=e.r+t.r,this.g=e.g+t.g,this.b=e.b+t.b,this}addScalar(e){return this.r+=e,this.g+=e,this.b+=e,this}sub(e){return this.r=Math.max(0,this.r-e.r),this.g=Math.max(0,this.g-e.g),this.b=Math.max(0,this.b-e.b),this}multiply(e){return this.r*=e.r,this.g*=e.g,this.b*=e.b,this}multiplyScalar(e){return this.r*=e,this.g*=e,this.b*=e,this}lerp(e,t){return this.r+=(e.r-this.r)*t,this.g+=(e.g-this.g)*t,this.b+=(e.b-this.b)*t,this}lerpColors(e,t,i){return this.r=e.r+(t.r-e.r)*i,this.g=e.g+(t.g-e.g)*i,this.b=e.b+(t.b-e.b)*i,this}lerpHSL(e,t){this.getHSL(Li),e.getHSL(Ic);let i=lh(Li.h,Ic.h,t),r=lh(Li.s,Ic.s,t),s=lh(Li.l,Ic.l,t);return this.setHSL(i,r,s),this}setFromVector3(e){return this.r=e.x,this.g=e.y,this.b=e.z,this}applyMatrix3(e){let t=this.r,i=this.g,r=this.b,s=e.elements;return this.r=s[0]*t+s[3]*i+s[6]*r,this.g=s[1]*t+s[4]*i+s[7]*r,this.b=s[2]*t+s[5]*i+s[8]*r,this}equals(e){return e.r===this.r&&e.g===this.g&&e.b===this.b}fromArray(e,t=0){return this.r=e[t],this.g=e[t+1],this.b=e[t+2],this}toArray(e=[],t=0){return e[t]=this.r,e[t+1]=this.g,e[t+2]=this.b,e}fromBufferAttribute(e,t){return this.r=e.getX(t),this.g=e.getY(t),this.b=e.getZ(t),this}toJSON(){return this.getHex()}*[Symbol.iterator](){yield this.r,yield this.g,yield this.b}},Ht=new Ze;Ze.NAMES=h_;var Uo=class extends Rr{constructor(){super(),this.isScene=!0,this.type="Scene",this.background=null,this.environment=null,this.fog=null,this.backgroundBlurriness=0,this.backgroundIntensity=1,this.backgroundRotation=new Fo,this.environmentIntensity=1,this.environmentRotation=new Fo,this.overrideMaterial=null,typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}copy(e,t){return super.copy(e,t),e.background!==null&&(this.background=e.background.clone()),e.environment!==null&&(this.environment=e.environment.clone()),e.fog!==null&&(this.fog=e.fog.clone()),this.backgroundBlurriness=e.backgroundBlurriness,this.backgroundIntensity=e.backgroundIntensity,this.backgroundRotation.copy(e.backgroundRotation),this.environmentIntensity=e.environmentIntensity,this.environmentRotation.copy(e.environmentRotation),e.overrideMaterial!==null&&(this.overrideMaterial=e.overrideMaterial.clone()),this.matrixAutoUpdate=e.matrixAutoUpdate,this}toJSON(e){let t=super.toJSON(e);return this.fog!==null&&(t.object.fog=this.fog.toJSON()),this.backgroundBlurriness>0&&(t.object.backgroundBlurriness=this.backgroundBlurriness),this.backgroundIntensity!==1&&(t.object.backgroundIntensity=this.backgroundIntensity),t.object.backgroundRotation=this.backgroundRotation.toArray(),this.environmentIntensity!==1&&(t.object.environmentIntensity=this.environmentIntensity),t.object.environmentRotation=this.environmentRotation.toArray(),t}},Pn=new V,di=new V,gh=new V,fi=new V,ys=new V,_s=new V,by=new V,vh=new V,yh=new V,_h=new V,xh=new _t,Mh=new _t,Sh=new _t,Bi=class n{constructor(e=new V,t=new V,i=new V){this.a=e,this.b=t,this.c=i}static getNormal(e,t,i,r){r.subVectors(i,t),Pn.subVectors(e,t),r.cross(Pn);let s=r.lengthSq();return s>0?r.multiplyScalar(1/Math.sqrt(s)):r.set(0,0,0)}static getBarycoord(e,t,i,r,s){Pn.subVectors(r,t),di.subVectors(i,t),gh.subVectors(e,t);let o=Pn.dot(Pn),a=Pn.dot(di),c=Pn.dot(gh),l=di.dot(di),u=di.dot(gh),f=o*l-a*a;if(f===0)return s.set(0,0,0),null;let d=1/f,h=(l*c-a*u)*d,v=(o*u-a*c)*d;return s.set(1-h-v,v,h)}static containsPoint(e,t,i,r){return this.getBarycoord(e,t,i,r,fi)===null?!1:fi.x>=0&&fi.y>=0&&fi.x+fi.y<=1}static getInterpolation(e,t,i,r,s,o,a,c){return this.getBarycoord(e,t,i,r,fi)===null?(c.x=0,c.y=0,"z"in c&&(c.z=0),"w"in c&&(c.w=0),null):(c.setScalar(0),c.addScaledVector(s,fi.x),c.addScaledVector(o,fi.y),c.addScaledVector(a,fi.z),c)}static getInterpolatedAttribute(e,t,i,r,s,o){return xh.setScalar(0),Mh.setScalar(0),Sh.setScalar(0),xh.fromBufferAttribute(e,t),Mh.fromBufferAttribute(e,i),Sh.fromBufferAttribute(e,r),o.setScalar(0),o.addScaledVector(xh,s.x),o.addScaledVector(Mh,s.y),o.addScaledVector(Sh,s.z),o}static isFrontFacing(e,t,i,r){return Pn.subVectors(i,t),di.subVectors(e,t),Pn.cross(di).dot(r)<0}set(e,t,i){return this.a.copy(e),this.b.copy(t),this.c.copy(i),this}setFromPointsAndIndices(e,t,i,r){return this.a.copy(e[t]),this.b.copy(e[i]),this.c.copy(e[r]),this}setFromAttributeAndIndices(e,t,i,r){return this.a.fromBufferAttribute(e,t),this.b.fromBufferAttribute(e,i),this.c.fromBufferAttribute(e,r),this}clone(){return new this.constructor().copy(this)}copy(e){return this.a.copy(e.a),this.b.copy(e.b),this.c.copy(e.c),this}getArea(){return Pn.subVectors(this.c,this.b),di.subVectors(this.a,this.b),Pn.cross(di).length()*.5}getMidpoint(e){return e.addVectors(this.a,this.b).add(this.c).multiplyScalar(1/3)}getNormal(e){return n.getNormal(this.a,this.b,this.c,e)}getPlane(e){return e.setFromCoplanarPoints(this.a,this.b,this.c)}getBarycoord(e,t){return n.getBarycoord(e,this.a,this.b,this.c,t)}getInterpolation(e,t,i,r,s){return n.getInterpolation(e,this.a,this.b,this.c,t,i,r,s)}containsPoint(e){return n.containsPoint(e,this.a,this.b,this.c)}isFrontFacing(e){return n.isFrontFacing(this.a,this.b,this.c,e)}intersectsBox(e){return e.intersectsTriangle(this)}closestPointToPoint(e,t){let i=this.a,r=this.b,s=this.c,o,a;ys.subVectors(r,i),_s.subVectors(s,i),vh.subVectors(e,i);let c=ys.dot(vh),l=_s.dot(vh);if(c<=0&&l<=0)return t.copy(i);yh.subVectors(e,r);let u=ys.dot(yh),f=_s.dot(yh);if(u>=0&&f<=u)return t.copy(r);let d=c*f-u*l;if(d<=0&&c>=0&&u<=0)return o=c/(c-u),t.copy(i).addScaledVector(ys,o);_h.subVectors(e,s);let h=ys.dot(_h),v=_s.dot(_h);if(v>=0&&h<=v)return t.copy(s);let M=h*l-c*v;if(M<=0&&l>=0&&v<=0)return a=l/(l-v),t.copy(i).addScaledVector(_s,a);let m=u*v-h*f;if(m<=0&&f-u>=0&&h-v>=0)return by.subVectors(s,r),a=(f-u)/(f-u+(h-v)),t.copy(r).addScaledVector(by,a);let p=1/(m+M+d);return o=M*p,a=d*p,t.copy(i).addScaledVector(ys,o).addScaledVector(_s,a)}equals(e){return e.a.equals(this.a)&&e.b.equals(this.b)&&e.c.equals(this.c)}},Hi=class{constructor(e=new V(1/0,1/0,1/0),t=new V(-1/0,-1/0,-1/0)){this.isBox3=!0,this.min=e,this.max=t}set(e,t){return this.min.copy(e),this.max.copy(t),this}setFromArray(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t+=3)this.expandByPoint(Ln.fromArray(e,t));return this}setFromBufferAttribute(e){this.makeEmpty();for(let t=0,i=e.count;t<i;t++)this.expandByPoint(Ln.fromBufferAttribute(e,t));return this}setFromPoints(e){this.makeEmpty();for(let t=0,i=e.length;t<i;t++)this.expandByPoint(e[t]);return this}setFromCenterAndSize(e,t){let i=Ln.copy(t).multiplyScalar(.5);return this.min.copy(e).sub(i),this.max.copy(e).add(i),this}setFromObject(e,t=!1){return this.makeEmpty(),this.expandByObject(e,t)}clone(){return new this.constructor().copy(this)}copy(e){return this.min.copy(e.min),this.max.copy(e.max),this}makeEmpty(){return this.min.x=this.min.y=this.min.z=1/0,this.max.x=this.max.y=this.max.z=-1/0,this}isEmpty(){return this.max.x<this.min.x||this.max.y<this.min.y||this.max.z<this.min.z}getCenter(e){return this.isEmpty()?e.set(0,0,0):e.addVectors(this.min,this.max).multiplyScalar(.5)}getSize(e){return this.isEmpty()?e.set(0,0,0):e.subVectors(this.max,this.min)}expandByPoint(e){return this.min.min(e),this.max.max(e),this}expandByVector(e){return this.min.sub(e),this.max.add(e),this}expandByScalar(e){return this.min.addScalar(-e),this.max.addScalar(e),this}expandByObject(e,t=!1){e.updateWorldMatrix(!1,!1);let i=e.geometry;if(i!==void 0){let s=i.getAttribute("position");if(t===!0&&s!==void 0&&e.isInstancedMesh!==!0)for(let o=0,a=s.count;o<a;o++)e.isMesh===!0?e.getVertexPosition(o,Ln):Ln.fromBufferAttribute(s,o),Ln.applyMatrix4(e.matrixWorld),this.expandByPoint(Ln);else e.boundingBox!==void 0?(e.boundingBox===null&&e.computeBoundingBox(),Cc.copy(e.boundingBox)):(i.boundingBox===null&&i.computeBoundingBox(),Cc.copy(i.boundingBox)),Cc.applyMatrix4(e.matrixWorld),this.union(Cc)}let r=e.children;for(let s=0,o=r.length;s<o;s++)this.expandByObject(r[s],t);return this}containsPoint(e){return e.x>=this.min.x&&e.x<=this.max.x&&e.y>=this.min.y&&e.y<=this.max.y&&e.z>=this.min.z&&e.z<=this.max.z}containsBox(e){return this.min.x<=e.min.x&&e.max.x<=this.max.x&&this.min.y<=e.min.y&&e.max.y<=this.max.y&&this.min.z<=e.min.z&&e.max.z<=this.max.z}getParameter(e,t){return t.set((e.x-this.min.x)/(this.max.x-this.min.x),(e.y-this.min.y)/(this.max.y-this.min.y),(e.z-this.min.z)/(this.max.z-this.min.z))}intersectsBox(e){return e.max.x>=this.min.x&&e.min.x<=this.max.x&&e.max.y>=this.min.y&&e.min.y<=this.max.y&&e.max.z>=this.min.z&&e.min.z<=this.max.z}intersectsSphere(e){return this.clampPoint(e.center,Ln),Ln.distanceToSquared(e.center)<=e.radius*e.radius}intersectsPlane(e){let t,i;return e.normal.x>0?(t=e.normal.x*this.min.x,i=e.normal.x*this.max.x):(t=e.normal.x*this.max.x,i=e.normal.x*this.min.x),e.normal.y>0?(t+=e.normal.y*this.min.y,i+=e.normal.y*this.max.y):(t+=e.normal.y*this.max.y,i+=e.normal.y*this.min.y),e.normal.z>0?(t+=e.normal.z*this.min.z,i+=e.normal.z*this.max.z):(t+=e.normal.z*this.max.z,i+=e.normal.z*this.min.z),t<=-e.constant&&i>=-e.constant}intersectsTriangle(e){if(this.isEmpty())return!1;this.getCenter(Io),Ac.subVectors(this.max,Io),xs.subVectors(e.a,Io),Ms.subVectors(e.b,Io),Ss.subVectors(e.c,Io),Oi.subVectors(Ms,xs),Fi.subVectors(Ss,Ms),Mr.subVectors(xs,Ss);let t=[0,-Oi.z,Oi.y,0,-Fi.z,Fi.y,0,-Mr.z,Mr.y,Oi.z,0,-Oi.x,Fi.z,0,-Fi.x,Mr.z,0,-Mr.x,-Oi.y,Oi.x,0,-Fi.y,Fi.x,0,-Mr.y,Mr.x,0];return!Eh(t,xs,Ms,Ss,Ac)||(t=[1,0,0,0,1,0,0,0,1],!Eh(t,xs,Ms,Ss,Ac))?!1:(Dc.crossVectors(Oi,Fi),t=[Dc.x,Dc.y,Dc.z],Eh(t,xs,Ms,Ss,Ac))}clampPoint(e,t){return t.copy(e).clamp(this.min,this.max)}distanceToPoint(e){return this.clampPoint(e,Ln).distanceTo(e)}getBoundingSphere(e){return this.isEmpty()?e.makeEmpty():(this.getCenter(e.center),e.radius=this.getSize(Ln).length()*.5),e}intersect(e){return this.min.max(e.min),this.max.min(e.max),this.isEmpty()&&this.makeEmpty(),this}union(e){return this.min.min(e.min),this.max.max(e.max),this}applyMatrix4(e){return this.isEmpty()?this:(hi[0].set(this.min.x,this.min.y,this.min.z).applyMatrix4(e),hi[1].set(this.min.x,this.min.y,this.max.z).applyMatrix4(e),hi[2].set(this.min.x,this.max.y,this.min.z).applyMatrix4(e),hi[3].set(this.min.x,this.max.y,this.max.z).applyMatrix4(e),hi[4].set(this.max.x,this.min.y,this.min.z).applyMatrix4(e),hi[5].set(this.max.x,this.min.y,this.max.z).applyMatrix4(e),hi[6].set(this.max.x,this.max.y,this.min.z).applyMatrix4(e),hi[7].set(this.max.x,this.max.y,this.max.z).applyMatrix4(e),this.setFromPoints(hi),this)}translate(e){return this.min.add(e),this.max.add(e),this}equals(e){return e.min.equals(this.min)&&e.max.equals(this.max)}toJSON(){return{min:this.min.toArray(),max:this.max.toArray()}}fromJSON(e){return this.min.fromArray(e.min),this.max.fromArray(e.max),this}},hi=[new V,new V,new V,new V,new V,new V,new V,new V],Ln=new V,Cc=new Hi,xs=new V,Ms=new V,Ss=new V,Oi=new V,Fi=new V,Mr=new V,Io=new V,Ac=new V,Dc=new V,Sr=new V;function Eh(n,e,t,i,r){for(let s=0,o=n.length-3;s<=o;s+=3){Sr.fromArray(n,s);let a=r.x*Math.abs(Sr.x)+r.y*Math.abs(Sr.y)+r.z*Math.abs(Sr.z),c=e.dot(Sr),l=t.dot(Sr),u=i.dot(Sr);if(Math.max(-Math.max(c,l,u),Math.min(c,l,u))>a)return!1}return!0}var bt=new V,Rc=new tt,hb=0,$t=class extends Jn{constructor(e,t,i=!1){if(super(),Array.isArray(e))throw new TypeError("THREE.BufferAttribute: array should be a Typed Array.");this.isBufferAttribute=!0,Object.defineProperty(this,"id",{value:hb++}),this.name="",this.array=e,this.itemSize=t,this.count=e!==void 0?e.length/t:0,this.normalized=i,this.usage=kh,this.updateRanges=[],this.gpuType=Un,this.version=0}onUploadCallback(){}set needsUpdate(e){e===!0&&this.version++}setUsage(e){return this.usage=e,this}addUpdateRange(e,t){this.updateRanges.push({start:e,count:t})}clearUpdateRanges(){this.updateRanges.length=0}copy(e){return this.name=e.name,this.array=new e.array.constructor(e.array),this.itemSize=e.itemSize,this.count=e.count,this.normalized=e.normalized,this.usage=e.usage,this.gpuType=e.gpuType,this}copyAt(e,t,i){e*=this.itemSize,i*=t.itemSize;for(let r=0,s=this.itemSize;r<s;r++)this.array[e+r]=t.array[i+r];return this}copyArray(e){return this.array.set(e),this}applyMatrix3(e){if(this.itemSize===2)for(let t=0,i=this.count;t<i;t++)Rc.fromBufferAttribute(this,t),Rc.applyMatrix3(e),this.setXY(t,Rc.x,Rc.y);else if(this.itemSize===3)for(let t=0,i=this.count;t<i;t++)bt.fromBufferAttribute(this,t),bt.applyMatrix3(e),this.setXYZ(t,bt.x,bt.y,bt.z);return this}applyMatrix4(e){for(let t=0,i=this.count;t<i;t++)bt.fromBufferAttribute(this,t),bt.applyMatrix4(e),this.setXYZ(t,bt.x,bt.y,bt.z);return this}applyNormalMatrix(e){for(let t=0,i=this.count;t<i;t++)bt.fromBufferAttribute(this,t),bt.applyNormalMatrix(e),this.setXYZ(t,bt.x,bt.y,bt.z);return this}transformDirection(e){for(let t=0,i=this.count;t<i;t++)bt.fromBufferAttribute(this,t),bt.transformDirection(e),this.setXYZ(t,bt.x,bt.y,bt.z);return this}set(e,t=0){return this.array.set(e,t),this}getComponent(e,t){let i=this.array[e*this.itemSize+t];return this.normalized&&(i=To(i,this.array)),i}setComponent(e,t,i){return this.normalized&&(i=Jt(i,this.array)),this.array[e*this.itemSize+t]=i,this}getX(e){let t=this.array[e*this.itemSize];return this.normalized&&(t=To(t,this.array)),t}setX(e,t){return this.normalized&&(t=Jt(t,this.array)),this.array[e*this.itemSize]=t,this}getY(e){let t=this.array[e*this.itemSize+1];return this.normalized&&(t=To(t,this.array)),t}setY(e,t){return this.normalized&&(t=Jt(t,this.array)),this.array[e*this.itemSize+1]=t,this}getZ(e){let t=this.array[e*this.itemSize+2];return this.normalized&&(t=To(t,this.array)),t}setZ(e,t){return this.normalized&&(t=Jt(t,this.array)),this.array[e*this.itemSize+2]=t,this}getW(e){let t=this.array[e*this.itemSize+3];return this.normalized&&(t=To(t,this.array)),t}setW(e,t){return this.normalized&&(t=Jt(t,this.array)),this.array[e*this.itemSize+3]=t,this}setXY(e,t,i){return e*=this.itemSize,this.normalized&&(t=Jt(t,this.array),i=Jt(i,this.array)),this.array[e+0]=t,this.array[e+1]=i,this}setXYZ(e,t,i,r){return e*=this.itemSize,this.normalized&&(t=Jt(t,this.array),i=Jt(i,this.array),r=Jt(r,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this}setXYZW(e,t,i,r,s){return e*=this.itemSize,this.normalized&&(t=Jt(t,this.array),i=Jt(i,this.array),r=Jt(r,this.array),s=Jt(s,this.array)),this.array[e+0]=t,this.array[e+1]=i,this.array[e+2]=r,this.array[e+3]=s,this}onUpload(e){return this.onUploadCallback=e,this}clone(){return new this.constructor(this.array,this.itemSize).copy(this)}toJSON(){let e={itemSize:this.itemSize,type:this.array.constructor.name,array:Array.from(this.array),normalized:this.normalized};return this.name!==""&&(e.name=this.name),this.usage!==kh&&(e.usage=this.usage),e}dispose(){this.dispatchEvent({type:"dispose"})}};var Bo=class extends $t{constructor(e,t,i){super(new Uint16Array(e),t,i)}};var Vo=class extends $t{constructor(e,t,i){super(new Uint32Array(e),t,i)}};var Mn=class extends $t{constructor(e,t,i){super(new Float32Array(e),t,i)}},pb=new Hi,Co=new V,bh=new V,Ds=class{constructor(e=new V,t=-1){this.isSphere=!0,this.center=e,this.radius=t}set(e,t){return this.center.copy(e),this.radius=t,this}setFromPoints(e,t){let i=this.center;t!==void 0?i.copy(t):pb.setFromPoints(e).getCenter(i);let r=0;for(let s=0,o=e.length;s<o;s++)r=Math.max(r,i.distanceToSquared(e[s]));return this.radius=Math.sqrt(r),this}copy(e){return this.center.copy(e.center),this.radius=e.radius,this}isEmpty(){return this.radius<0}makeEmpty(){return this.center.set(0,0,0),this.radius=-1,this}containsPoint(e){return e.distanceToSquared(this.center)<=this.radius*this.radius}distanceToPoint(e){return e.distanceTo(this.center)-this.radius}intersectsSphere(e){let t=this.radius+e.radius;return e.center.distanceToSquared(this.center)<=t*t}intersectsBox(e){return e.intersectsSphere(this)}intersectsPlane(e){return Math.abs(e.distanceToPoint(this.center))<=this.radius}clampPoint(e,t){let i=this.center.distanceToSquared(e);return t.copy(e),i>this.radius*this.radius&&(t.sub(this.center).normalize(),t.multiplyScalar(this.radius).add(this.center)),t}getBoundingBox(e){return this.isEmpty()?(e.makeEmpty(),e):(e.set(this.center,this.center),e.expandByScalar(this.radius),e)}applyMatrix4(e){return this.center.applyMatrix4(e),this.radius=this.radius*e.getMaxScaleOnAxis(),this}translate(e){return this.center.add(e),this}expandByPoint(e){if(this.isEmpty())return this.center.copy(e),this.radius=0,this;Co.subVectors(e,this.center);let t=Co.lengthSq();if(t>this.radius*this.radius){let i=Math.sqrt(t),r=(i-this.radius)*.5;this.center.addScaledVector(Co,r/i),this.radius+=r}return this}union(e){return e.isEmpty()?this:this.isEmpty()?(this.copy(e),this):(this.center.equals(e.center)===!0?this.radius=Math.max(this.radius,e.radius):(bh.subVectors(e.center,this.center).setLength(e.radius),this.expandByPoint(Co.copy(e.center).add(bh)),this.expandByPoint(Co.copy(e.center).sub(bh))),this)}equals(e){return e.center.equals(this.center)&&e.radius===this.radius}clone(){return new this.constructor().copy(this)}toJSON(){return{radius:this.radius,center:this.center.toArray()}}fromJSON(e){return this.radius=e.radius,this.center.fromArray(e.center),this}},mb=0,xn=new Tt,Th=new Rr,Es=new V,ln=new Hi,Ao=new Hi,Nt=new V,Sn=class n extends Jn{constructor(){super(),this.isBufferGeometry=!0,Object.defineProperty(this,"id",{value:mb++}),this.uuid=ia(),this.name="",this.type="BufferGeometry",this.index=null,this.indirect=null,this.indirectOffset=0,this.attributes={},this.morphAttributes={},this.morphTargetsRelative=!1,this.groups=[],this.boundingBox=null,this.boundingSphere=null,this.drawRange={start:0,count:1/0},this.userData={}}getIndex(){return this.index}setIndex(e){return Array.isArray(e)?this.index=new(eb(e)?Vo:Bo)(e,1):this.index=e,this}setIndirect(e,t=0){return this.indirect=e,this.indirectOffset=t,this}getIndirect(){return this.indirect}getAttribute(e){return this.attributes[e]}setAttribute(e,t){return this.attributes[e]=t,this}deleteAttribute(e){return delete this.attributes[e],this}hasAttribute(e){return this.attributes[e]!==void 0}addGroup(e,t,i=0){this.groups.push({start:e,count:t,materialIndex:i})}clearGroups(){this.groups=[]}setDrawRange(e,t){this.drawRange.start=e,this.drawRange.count=t}applyMatrix4(e){let t=this.attributes.position;t!==void 0&&(t.applyMatrix4(e),t.needsUpdate=!0);let i=this.attributes.normal;if(i!==void 0){let s=new Ae().getNormalMatrix(e);i.applyNormalMatrix(s),i.needsUpdate=!0}let r=this.attributes.tangent;return r!==void 0&&(r.transformDirection(e),r.needsUpdate=!0),this.boundingBox!==null&&this.computeBoundingBox(),this.boundingSphere!==null&&this.computeBoundingSphere(),this}applyQuaternion(e){return xn.makeRotationFromQuaternion(e),this.applyMatrix4(xn),this}rotateX(e){return xn.makeRotationX(e),this.applyMatrix4(xn),this}rotateY(e){return xn.makeRotationY(e),this.applyMatrix4(xn),this}rotateZ(e){return xn.makeRotationZ(e),this.applyMatrix4(xn),this}translate(e,t,i){return xn.makeTranslation(e,t,i),this.applyMatrix4(xn),this}scale(e,t,i){return xn.makeScale(e,t,i),this.applyMatrix4(xn),this}lookAt(e){return Th.lookAt(e),Th.updateMatrix(),this.applyMatrix4(Th.matrix),this}center(){return this.computeBoundingBox(),this.boundingBox.getCenter(Es).negate(),this.translate(Es.x,Es.y,Es.z),this}setFromPoints(e){let t=this.getAttribute("position");if(t===void 0){let i=[];for(let r=0,s=e.length;r<s;r++){let o=e[r];i.push(o.x,o.y,o.z||0)}this.setAttribute("position",new Mn(i,3))}else{let i=Math.min(e.length,t.count);for(let r=0;r<i;r++){let s=e[r];t.setXYZ(r,s.x,s.y,s.z||0)}e.length>t.count&&be("BufferGeometry: Buffer size too small for points data. Use .dispose() and create a new geometry."),t.needsUpdate=!0}return this}computeBoundingBox(){this.boundingBox===null&&(this.boundingBox=new Hi);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){we("BufferGeometry.computeBoundingBox(): GLBufferAttribute requires a manual bounding box.",this),this.boundingBox.set(new V(-1/0,-1/0,-1/0),new V(1/0,1/0,1/0));return}if(e!==void 0){if(this.boundingBox.setFromBufferAttribute(e),t)for(let i=0,r=t.length;i<r;i++){let s=t[i];ln.setFromBufferAttribute(s),this.morphTargetsRelative?(Nt.addVectors(this.boundingBox.min,ln.min),this.boundingBox.expandByPoint(Nt),Nt.addVectors(this.boundingBox.max,ln.max),this.boundingBox.expandByPoint(Nt)):(this.boundingBox.expandByPoint(ln.min),this.boundingBox.expandByPoint(ln.max))}}else this.boundingBox.makeEmpty();(isNaN(this.boundingBox.min.x)||isNaN(this.boundingBox.min.y)||isNaN(this.boundingBox.min.z))&&we('BufferGeometry.computeBoundingBox(): Computed min/max have NaN values. The "position" attribute is likely to have NaN values.',this)}computeBoundingSphere(){this.boundingSphere===null&&(this.boundingSphere=new Ds);let e=this.attributes.position,t=this.morphAttributes.position;if(e&&e.isGLBufferAttribute){we("BufferGeometry.computeBoundingSphere(): GLBufferAttribute requires a manual bounding sphere.",this),this.boundingSphere.set(new V,1/0);return}if(e){let i=this.boundingSphere.center;if(ln.setFromBufferAttribute(e),t)for(let s=0,o=t.length;s<o;s++){let a=t[s];Ao.setFromBufferAttribute(a),this.morphTargetsRelative?(Nt.addVectors(ln.min,Ao.min),ln.expandByPoint(Nt),Nt.addVectors(ln.max,Ao.max),ln.expandByPoint(Nt)):(ln.expandByPoint(Ao.min),ln.expandByPoint(Ao.max))}ln.getCenter(i);let r=0;for(let s=0,o=e.count;s<o;s++)Nt.fromBufferAttribute(e,s),r=Math.max(r,i.distanceToSquared(Nt));if(t)for(let s=0,o=t.length;s<o;s++){let a=t[s],c=this.morphTargetsRelative;for(let l=0,u=a.count;l<u;l++)Nt.fromBufferAttribute(a,l),c&&(Es.fromBufferAttribute(e,l),Nt.add(Es)),r=Math.max(r,i.distanceToSquared(Nt))}this.boundingSphere.radius=Math.sqrt(r),isNaN(this.boundingSphere.radius)&&we('BufferGeometry.computeBoundingSphere(): Computed radius is NaN. The "position" attribute is likely to have NaN values.',this)}}computeTangents(){let e=this.index,t=this.attributes;if(e===null||t.position===void 0||t.normal===void 0||t.uv===void 0){we("BufferGeometry: .computeTangents() failed. Missing required attributes (index, position, normal or uv)");return}let i=t.position,r=t.normal,s=t.uv;this.hasAttribute("tangent")===!1&&this.setAttribute("tangent",new $t(new Float32Array(4*i.count),4));let o=this.getAttribute("tangent"),a=[],c=[];for(let y=0;y<i.count;y++)a[y]=new V,c[y]=new V;let l=new V,u=new V,f=new V,d=new tt,h=new tt,v=new tt,M=new V,m=new V;function p(y,w,L){l.fromBufferAttribute(i,y),u.fromBufferAttribute(i,w),f.fromBufferAttribute(i,L),d.fromBufferAttribute(s,y),h.fromBufferAttribute(s,w),v.fromBufferAttribute(s,L),u.sub(l),f.sub(l),h.sub(d),v.sub(d);let I=1/(h.x*v.y-v.x*h.y);isFinite(I)&&(M.copy(u).multiplyScalar(v.y).addScaledVector(f,-h.y).multiplyScalar(I),m.copy(f).multiplyScalar(h.x).addScaledVector(u,-v.x).multiplyScalar(I),a[y].add(M),a[w].add(M),a[L].add(M),c[y].add(m),c[w].add(m),c[L].add(m))}let S=this.groups;S.length===0&&(S=[{start:0,count:e.count}]);for(let y=0,w=S.length;y<w;++y){let L=S[y],I=L.start,k=L.count;for(let W=I,X=I+k;W<X;W+=3)p(e.getX(W+0),e.getX(W+1),e.getX(W+2))}let T=new V,E=new V,D=new V,b=new V;function A(y){D.fromBufferAttribute(r,y),b.copy(D);let w=a[y];T.copy(w),T.sub(D.multiplyScalar(D.dot(w))).normalize(),E.crossVectors(b,w);let I=E.dot(c[y])<0?-1:1;o.setXYZW(y,T.x,T.y,T.z,I)}for(let y=0,w=S.length;y<w;++y){let L=S[y],I=L.start,k=L.count;for(let W=I,X=I+k;W<X;W+=3)A(e.getX(W+0)),A(e.getX(W+1)),A(e.getX(W+2))}}computeVertexNormals(){let e=this.index,t=this.getAttribute("position");if(t!==void 0){let i=this.getAttribute("normal");if(i===void 0)i=new $t(new Float32Array(t.count*3),3),this.setAttribute("normal",i);else for(let d=0,h=i.count;d<h;d++)i.setXYZ(d,0,0,0);let r=new V,s=new V,o=new V,a=new V,c=new V,l=new V,u=new V,f=new V;if(e)for(let d=0,h=e.count;d<h;d+=3){let v=e.getX(d+0),M=e.getX(d+1),m=e.getX(d+2);r.fromBufferAttribute(t,v),s.fromBufferAttribute(t,M),o.fromBufferAttribute(t,m),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),a.fromBufferAttribute(i,v),c.fromBufferAttribute(i,M),l.fromBufferAttribute(i,m),a.add(u),c.add(u),l.add(u),i.setXYZ(v,a.x,a.y,a.z),i.setXYZ(M,c.x,c.y,c.z),i.setXYZ(m,l.x,l.y,l.z)}else for(let d=0,h=t.count;d<h;d+=3)r.fromBufferAttribute(t,d+0),s.fromBufferAttribute(t,d+1),o.fromBufferAttribute(t,d+2),u.subVectors(o,s),f.subVectors(r,s),u.cross(f),i.setXYZ(d+0,u.x,u.y,u.z),i.setXYZ(d+1,u.x,u.y,u.z),i.setXYZ(d+2,u.x,u.y,u.z);this.normalizeNormals(),i.needsUpdate=!0}}normalizeNormals(){let e=this.attributes.normal;for(let t=0,i=e.count;t<i;t++)Nt.fromBufferAttribute(e,t),Nt.normalize(),e.setXYZ(t,Nt.x,Nt.y,Nt.z)}toNonIndexed(){function e(a,c){let l=a.array,u=a.itemSize,f=a.normalized,d=new l.constructor(c.length*u),h=0,v=0;for(let M=0,m=c.length;M<m;M++){a.isInterleavedBufferAttribute?h=c[M]*a.data.stride+a.offset:h=c[M]*u;for(let p=0;p<u;p++)d[v++]=l[h++]}return new $t(d,u,f)}if(this.index===null)return be("BufferGeometry.toNonIndexed(): BufferGeometry is already non-indexed."),this;let t=new n,i=this.index.array,r=this.attributes;for(let a in r){let c=r[a],l=e(c,i);t.setAttribute(a,l)}let s=this.morphAttributes;for(let a in s){let c=[],l=s[a];for(let u=0,f=l.length;u<f;u++){let d=l[u],h=e(d,i);c.push(h)}t.morphAttributes[a]=c}t.morphTargetsRelative=this.morphTargetsRelative;let o=this.groups;for(let a=0,c=o.length;a<c;a++){let l=o[a];t.addGroup(l.start,l.count,l.materialIndex)}return t}toJSON(){let e={metadata:{version:4.7,type:"BufferGeometry",generator:"BufferGeometry.toJSON"}};if(e.uuid=this.uuid,e.type=this.type,this.name!==""&&(e.name=this.name),Object.keys(this.userData).length>0&&(e.userData=this.userData),this.parameters!==void 0){let c=this.parameters;for(let l in c)c[l]!==void 0&&(e[l]=c[l]);return e}e.data={attributes:{}};let t=this.index;t!==null&&(e.data.index={type:t.array.constructor.name,array:Array.prototype.slice.call(t.array)});let i=this.attributes;for(let c in i){let l=i[c];e.data.attributes[c]=l.toJSON(e.data)}let r={},s=!1;for(let c in this.morphAttributes){let l=this.morphAttributes[c],u=[];for(let f=0,d=l.length;f<d;f++){let h=l[f];u.push(h.toJSON(e.data))}u.length>0&&(r[c]=u,s=!0)}s&&(e.data.morphAttributes=r,e.data.morphTargetsRelative=this.morphTargetsRelative);let o=this.groups;o.length>0&&(e.data.groups=JSON.parse(JSON.stringify(o)));let a=this.boundingSphere;return a!==null&&(e.data.boundingSphere=a.toJSON()),e}clone(){return new this.constructor().copy(this)}copy(e){this.index=null,this.attributes={},this.morphAttributes={},this.groups=[],this.boundingBox=null,this.boundingSphere=null;let t={};this.name=e.name;let i=e.index;i!==null&&this.setIndex(i.clone());let r=e.attributes;for(let l in r){let u=r[l];this.setAttribute(l,u.clone(t))}let s=e.morphAttributes;for(let l in s){let u=[],f=s[l];for(let d=0,h=f.length;d<h;d++)u.push(f[d].clone(t));this.morphAttributes[l]=u}this.morphTargetsRelative=e.morphTargetsRelative;let o=e.groups;for(let l=0,u=o.length;l<u;l++){let f=o[l];this.addGroup(f.start,f.count,f.materialIndex)}let a=e.boundingBox;a!==null&&(this.boundingBox=a.clone());let c=e.boundingSphere;return c!==null&&(this.boundingSphere=c.clone()),this.drawRange.start=e.drawRange.start,this.drawRange.count=e.drawRange.count,this.userData=e.userData,this}dispose(){this.dispatchEvent({type:"dispose"})}};var gb=0,vi=class extends Jn{constructor(){super(),this.isMaterial=!0,Object.defineProperty(this,"id",{value:gb++}),this.uuid=ia(),this.name="",this.type="Material",this.blending=Ir,this.side=gi,this.vertexColors=!1,this.opacity=1,this.transparent=!1,this.alphaHash=!1,this.blendSrc=$c,this.blendDst=qc,this.blendEquation=Vi,this.blendSrcAlpha=null,this.blendDstAlpha=null,this.blendEquationAlpha=null,this.blendColor=new Ze(0,0,0),this.blendAlpha=0,this.depthFunc=Cr,this.depthTest=!0,this.depthWrite=!0,this.stencilWriteMask=255,this.stencilFunc=Fh,this.stencilRef=0,this.stencilFuncMask=255,this.stencilFail=Tr,this.stencilZFail=Tr,this.stencilZPass=Tr,this.stencilWrite=!1,this.clippingPlanes=null,this.clipIntersection=!1,this.clipShadows=!1,this.shadowSide=null,this.colorWrite=!0,this.precision=null,this.polygonOffset=!1,this.polygonOffsetFactor=0,this.polygonOffsetUnits=0,this.dithering=!1,this.alphaToCoverage=!1,this.premultipliedAlpha=!1,this.forceSinglePass=!1,this.allowOverride=!0,this.visible=!0,this.toneMapped=!0,this.userData={},this.version=0,this._alphaTest=0}get alphaTest(){return this._alphaTest}set alphaTest(e){this._alphaTest>0!=e>0&&this.version++,this._alphaTest=e}onBeforeRender(){}onBeforeCompile(){}customProgramCacheKey(){return this.onBeforeCompile.toString()}setValues(e){if(e!==void 0)for(let t in e){let i=e[t];if(i===void 0){be(`Material: parameter '${t}' has value of undefined.`);continue}let r=this[t];if(r===void 0){be(`Material: '${t}' is not a property of THREE.${this.type}.`);continue}r&&r.isColor?r.set(i):r&&r.isVector3&&i&&i.isVector3?r.copy(i):this[t]=i}}toJSON(e){let t=e===void 0||typeof e=="string";t&&(e={textures:{},images:{}});let i={metadata:{version:4.7,type:"Material",generator:"Material.toJSON"}};i.uuid=this.uuid,i.type=this.type,this.name!==""&&(i.name=this.name),this.color&&this.color.isColor&&(i.color=this.color.getHex()),this.roughness!==void 0&&(i.roughness=this.roughness),this.metalness!==void 0&&(i.metalness=this.metalness),this.sheen!==void 0&&(i.sheen=this.sheen),this.sheenColor&&this.sheenColor.isColor&&(i.sheenColor=this.sheenColor.getHex()),this.sheenRoughness!==void 0&&(i.sheenRoughness=this.sheenRoughness),this.emissive&&this.emissive.isColor&&(i.emissive=this.emissive.getHex()),this.emissiveIntensity!==void 0&&this.emissiveIntensity!==1&&(i.emissiveIntensity=this.emissiveIntensity),this.specular&&this.specular.isColor&&(i.specular=this.specular.getHex()),this.specularIntensity!==void 0&&(i.specularIntensity=this.specularIntensity),this.specularColor&&this.specularColor.isColor&&(i.specularColor=this.specularColor.getHex()),this.shininess!==void 0&&(i.shininess=this.shininess),this.clearcoat!==void 0&&(i.clearcoat=this.clearcoat),this.clearcoatRoughness!==void 0&&(i.clearcoatRoughness=this.clearcoatRoughness),this.clearcoatMap&&this.clearcoatMap.isTexture&&(i.clearcoatMap=this.clearcoatMap.toJSON(e).uuid),this.clearcoatRoughnessMap&&this.clearcoatRoughnessMap.isTexture&&(i.clearcoatRoughnessMap=this.clearcoatRoughnessMap.toJSON(e).uuid),this.clearcoatNormalMap&&this.clearcoatNormalMap.isTexture&&(i.clearcoatNormalMap=this.clearcoatNormalMap.toJSON(e).uuid,i.clearcoatNormalScale=this.clearcoatNormalScale.toArray()),this.sheenColorMap&&this.sheenColorMap.isTexture&&(i.sheenColorMap=this.sheenColorMap.toJSON(e).uuid),this.sheenRoughnessMap&&this.sheenRoughnessMap.isTexture&&(i.sheenRoughnessMap=this.sheenRoughnessMap.toJSON(e).uuid),this.dispersion!==void 0&&(i.dispersion=this.dispersion),this.iridescence!==void 0&&(i.iridescence=this.iridescence),this.iridescenceIOR!==void 0&&(i.iridescenceIOR=this.iridescenceIOR),this.iridescenceThicknessRange!==void 0&&(i.iridescenceThicknessRange=this.iridescenceThicknessRange),this.iridescenceMap&&this.iridescenceMap.isTexture&&(i.iridescenceMap=this.iridescenceMap.toJSON(e).uuid),this.iridescenceThicknessMap&&this.iridescenceThicknessMap.isTexture&&(i.iridescenceThicknessMap=this.iridescenceThicknessMap.toJSON(e).uuid),this.anisotropy!==void 0&&(i.anisotropy=this.anisotropy),this.anisotropyRotation!==void 0&&(i.anisotropyRotation=this.anisotropyRotation),this.anisotropyMap&&this.anisotropyMap.isTexture&&(i.anisotropyMap=this.anisotropyMap.toJSON(e).uuid),this.map&&this.map.isTexture&&(i.map=this.map.toJSON(e).uuid),this.matcap&&this.matcap.isTexture&&(i.matcap=this.matcap.toJSON(e).uuid),this.alphaMap&&this.alphaMap.isTexture&&(i.alphaMap=this.alphaMap.toJSON(e).uuid),this.lightMap&&this.lightMap.isTexture&&(i.lightMap=this.lightMap.toJSON(e).uuid,i.lightMapIntensity=this.lightMapIntensity),this.aoMap&&this.aoMap.isTexture&&(i.aoMap=this.aoMap.toJSON(e).uuid,i.aoMapIntensity=this.aoMapIntensity),this.bumpMap&&this.bumpMap.isTexture&&(i.bumpMap=this.bumpMap.toJSON(e).uuid,i.bumpScale=this.bumpScale),this.normalMap&&this.normalMap.isTexture&&(i.normalMap=this.normalMap.toJSON(e).uuid,i.normalMapType=this.normalMapType,i.normalScale=this.normalScale.toArray()),this.displacementMap&&this.displacementMap.isTexture&&(i.displacementMap=this.displacementMap.toJSON(e).uuid,i.displacementScale=this.displacementScale,i.displacementBias=this.displacementBias),this.roughnessMap&&this.roughnessMap.isTexture&&(i.roughnessMap=this.roughnessMap.toJSON(e).uuid),this.metalnessMap&&this.metalnessMap.isTexture&&(i.metalnessMap=this.metalnessMap.toJSON(e).uuid),this.emissiveMap&&this.emissiveMap.isTexture&&(i.emissiveMap=this.emissiveMap.toJSON(e).uuid),this.specularMap&&this.specularMap.isTexture&&(i.specularMap=this.specularMap.toJSON(e).uuid),this.specularIntensityMap&&this.specularIntensityMap.isTexture&&(i.specularIntensityMap=this.specularIntensityMap.toJSON(e).uuid),this.specularColorMap&&this.specularColorMap.isTexture&&(i.specularColorMap=this.specularColorMap.toJSON(e).uuid),this.envMap&&this.envMap.isTexture&&(i.envMap=this.envMap.toJSON(e).uuid,this.combine!==void 0&&(i.combine=this.combine)),this.envMapRotation!==void 0&&(i.envMapRotation=this.envMapRotation.toArray()),this.envMapIntensity!==void 0&&(i.envMapIntensity=this.envMapIntensity),this.reflectivity!==void 0&&(i.reflectivity=this.reflectivity),this.refractionRatio!==void 0&&(i.refractionRatio=this.refractionRatio),this.gradientMap&&this.gradientMap.isTexture&&(i.gradientMap=this.gradientMap.toJSON(e).uuid),this.transmission!==void 0&&(i.transmission=this.transmission),this.transmissionMap&&this.transmissionMap.isTexture&&(i.transmissionMap=this.transmissionMap.toJSON(e).uuid),this.thickness!==void 0&&(i.thickness=this.thickness),this.thicknessMap&&this.thicknessMap.isTexture&&(i.thicknessMap=this.thicknessMap.toJSON(e).uuid),this.attenuationDistance!==void 0&&this.attenuationDistance!==1/0&&(i.attenuationDistance=this.attenuationDistance),this.attenuationColor!==void 0&&(i.attenuationColor=this.attenuationColor.getHex()),this.size!==void 0&&(i.size=this.size),this.shadowSide!==null&&(i.shadowSide=this.shadowSide),this.sizeAttenuation!==void 0&&(i.sizeAttenuation=this.sizeAttenuation),this.blending!==Ir&&(i.blending=this.blending),this.side!==gi&&(i.side=this.side),this.vertexColors===!0&&(i.vertexColors=!0),this.opacity<1&&(i.opacity=this.opacity),this.transparent===!0&&(i.transparent=!0),this.blendSrc!==$c&&(i.blendSrc=this.blendSrc),this.blendDst!==qc&&(i.blendDst=this.blendDst),this.blendEquation!==Vi&&(i.blendEquation=this.blendEquation),this.blendSrcAlpha!==null&&(i.blendSrcAlpha=this.blendSrcAlpha),this.blendDstAlpha!==null&&(i.blendDstAlpha=this.blendDstAlpha),this.blendEquationAlpha!==null&&(i.blendEquationAlpha=this.blendEquationAlpha),this.blendColor&&this.blendColor.isColor&&(i.blendColor=this.blendColor.getHex()),this.blendAlpha!==0&&(i.blendAlpha=this.blendAlpha),this.depthFunc!==Cr&&(i.depthFunc=this.depthFunc),this.depthTest===!1&&(i.depthTest=this.depthTest),this.depthWrite===!1&&(i.depthWrite=this.depthWrite),this.colorWrite===!1&&(i.colorWrite=this.colorWrite),this.stencilWriteMask!==255&&(i.stencilWriteMask=this.stencilWriteMask),this.stencilFunc!==Fh&&(i.stencilFunc=this.stencilFunc),this.stencilRef!==0&&(i.stencilRef=this.stencilRef),this.stencilFuncMask!==255&&(i.stencilFuncMask=this.stencilFuncMask),this.stencilFail!==Tr&&(i.stencilFail=this.stencilFail),this.stencilZFail!==Tr&&(i.stencilZFail=this.stencilZFail),this.stencilZPass!==Tr&&(i.stencilZPass=this.stencilZPass),this.stencilWrite===!0&&(i.stencilWrite=this.stencilWrite),this.rotation!==void 0&&this.rotation!==0&&(i.rotation=this.rotation),this.polygonOffset===!0&&(i.polygonOffset=!0),this.polygonOffsetFactor!==0&&(i.polygonOffsetFactor=this.polygonOffsetFactor),this.polygonOffsetUnits!==0&&(i.polygonOffsetUnits=this.polygonOffsetUnits),this.linewidth!==void 0&&this.linewidth!==1&&(i.linewidth=this.linewidth),this.dashSize!==void 0&&(i.dashSize=this.dashSize),this.gapSize!==void 0&&(i.gapSize=this.gapSize),this.scale!==void 0&&(i.scale=this.scale),this.dithering===!0&&(i.dithering=!0),this.alphaTest>0&&(i.alphaTest=this.alphaTest),this.alphaHash===!0&&(i.alphaHash=!0),this.alphaToCoverage===!0&&(i.alphaToCoverage=!0),this.premultipliedAlpha===!0&&(i.premultipliedAlpha=!0),this.forceSinglePass===!0&&(i.forceSinglePass=!0),this.allowOverride===!1&&(i.allowOverride=!1),this.wireframe===!0&&(i.wireframe=!0),this.wireframeLinewidth>1&&(i.wireframeLinewidth=this.wireframeLinewidth),this.wireframeLinecap!=="round"&&(i.wireframeLinecap=this.wireframeLinecap),this.wireframeLinejoin!=="round"&&(i.wireframeLinejoin=this.wireframeLinejoin),this.flatShading===!0&&(i.flatShading=!0),this.visible===!1&&(i.visible=!1),this.toneMapped===!1&&(i.toneMapped=!1),this.fog===!1&&(i.fog=!1),Object.keys(this.userData).length>0&&(i.userData=this.userData);function r(s){let o=[];for(let a in s){let c=s[a];delete c.metadata,o.push(c)}return o}if(t){let s=r(e.textures),o=r(e.images);s.length>0&&(i.textures=s),o.length>0&&(i.images=o)}return i}clone(){return new this.constructor().copy(this)}copy(e){this.name=e.name,this.blending=e.blending,this.side=e.side,this.vertexColors=e.vertexColors,this.opacity=e.opacity,this.transparent=e.transparent,this.blendSrc=e.blendSrc,this.blendDst=e.blendDst,this.blendEquation=e.blendEquation,this.blendSrcAlpha=e.blendSrcAlpha,this.blendDstAlpha=e.blendDstAlpha,this.blendEquationAlpha=e.blendEquationAlpha,this.blendColor.copy(e.blendColor),this.blendAlpha=e.blendAlpha,this.depthFunc=e.depthFunc,this.depthTest=e.depthTest,this.depthWrite=e.depthWrite,this.stencilWriteMask=e.stencilWriteMask,this.stencilFunc=e.stencilFunc,this.stencilRef=e.stencilRef,this.stencilFuncMask=e.stencilFuncMask,this.stencilFail=e.stencilFail,this.stencilZFail=e.stencilZFail,this.stencilZPass=e.stencilZPass,this.stencilWrite=e.stencilWrite;let t=e.clippingPlanes,i=null;if(t!==null){let r=t.length;i=new Array(r);for(let s=0;s!==r;++s)i[s]=t[s].clone()}return this.clippingPlanes=i,this.clipIntersection=e.clipIntersection,this.clipShadows=e.clipShadows,this.shadowSide=e.shadowSide,this.colorWrite=e.colorWrite,this.precision=e.precision,this.polygonOffset=e.polygonOffset,this.polygonOffsetFactor=e.polygonOffsetFactor,this.polygonOffsetUnits=e.polygonOffsetUnits,this.dithering=e.dithering,this.alphaTest=e.alphaTest,this.alphaHash=e.alphaHash,this.alphaToCoverage=e.alphaToCoverage,this.premultipliedAlpha=e.premultipliedAlpha,this.forceSinglePass=e.forceSinglePass,this.allowOverride=e.allowOverride,this.visible=e.visible,this.toneMapped=e.toneMapped,this.userData=JSON.parse(JSON.stringify(e.userData)),this}dispose(){this.dispatchEvent({type:"dispose"})}set needsUpdate(e){e===!0&&this.version++}};var pi=new V,wh=new V,Nc=new V,ki=new V,Ih=new V,Pc=new V,Ch=new V,ll=class{constructor(e=new V,t=new V(0,0,-1)){this.origin=e,this.direction=t}set(e,t){return this.origin.copy(e),this.direction.copy(t),this}copy(e){return this.origin.copy(e.origin),this.direction.copy(e.direction),this}at(e,t){return t.copy(this.origin).addScaledVector(this.direction,e)}lookAt(e){return this.direction.copy(e).sub(this.origin).normalize(),this}recast(e){return this.origin.copy(this.at(e,pi)),this}closestPointToPoint(e,t){t.subVectors(e,this.origin);let i=t.dot(this.direction);return i<0?t.copy(this.origin):t.copy(this.origin).addScaledVector(this.direction,i)}distanceToPoint(e){return Math.sqrt(this.distanceSqToPoint(e))}distanceSqToPoint(e){let t=pi.subVectors(e,this.origin).dot(this.direction);return t<0?this.origin.distanceToSquared(e):(pi.copy(this.origin).addScaledVector(this.direction,t),pi.distanceToSquared(e))}distanceSqToSegment(e,t,i,r){wh.copy(e).add(t).multiplyScalar(.5),Nc.copy(t).sub(e).normalize(),ki.copy(this.origin).sub(wh);let s=e.distanceTo(t)*.5,o=-this.direction.dot(Nc),a=ki.dot(this.direction),c=-ki.dot(Nc),l=ki.lengthSq(),u=Math.abs(1-o*o),f,d,h,v;if(u>0)if(f=o*c-a,d=o*a-c,v=s*u,f>=0)if(d>=-v)if(d<=v){let M=1/u;f*=M,d*=M,h=f*(f+o*d+2*a)+d*(o*f+d+2*c)+l}else d=s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d=-s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;else d<=-v?(f=Math.max(0,-(-o*s+a)),d=f>0?-s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l):d<=v?(f=0,d=Math.min(Math.max(-s,-c),s),h=d*(d+2*c)+l):(f=Math.max(0,-(o*s+a)),d=f>0?s:Math.min(Math.max(-s,-c),s),h=-f*f+d*(d+2*c)+l);else d=o>0?-s:s,f=Math.max(0,-(o*d+a)),h=-f*f+d*(d+2*c)+l;return i&&i.copy(this.origin).addScaledVector(this.direction,f),r&&r.copy(wh).addScaledVector(Nc,d),h}intersectSphere(e,t){pi.subVectors(e.center,this.origin);let i=pi.dot(this.direction),r=pi.dot(pi)-i*i,s=e.radius*e.radius;if(r>s)return null;let o=Math.sqrt(s-r),a=i-o,c=i+o;return c<0?null:a<0?this.at(c,t):this.at(a,t)}intersectsSphere(e){return e.radius<0?!1:this.distanceSqToPoint(e.center)<=e.radius*e.radius}distanceToPlane(e){let t=e.normal.dot(this.direction);if(t===0)return e.distanceToPoint(this.origin)===0?0:null;let i=-(this.origin.dot(e.normal)+e.constant)/t;return i>=0?i:null}intersectPlane(e,t){let i=this.distanceToPlane(e);return i===null?null:this.at(i,t)}intersectsPlane(e){let t=e.distanceToPoint(this.origin);return t===0||e.normal.dot(this.direction)*t<0}intersectBox(e,t){let i,r,s,o,a,c,l=1/this.direction.x,u=1/this.direction.y,f=1/this.direction.z,d=this.origin;return l>=0?(i=(e.min.x-d.x)*l,r=(e.max.x-d.x)*l):(i=(e.max.x-d.x)*l,r=(e.min.x-d.x)*l),u>=0?(s=(e.min.y-d.y)*u,o=(e.max.y-d.y)*u):(s=(e.max.y-d.y)*u,o=(e.min.y-d.y)*u),i>o||s>r||((s>i||isNaN(i))&&(i=s),(o<r||isNaN(r))&&(r=o),f>=0?(a=(e.min.z-d.z)*f,c=(e.max.z-d.z)*f):(a=(e.max.z-d.z)*f,c=(e.min.z-d.z)*f),i>c||a>r)||((a>i||i!==i)&&(i=a),(c<r||r!==r)&&(r=c),r<0)?null:this.at(i>=0?i:r,t)}intersectsBox(e){return this.intersectBox(e,pi)!==null}intersectTriangle(e,t,i,r,s){Ih.subVectors(t,e),Pc.subVectors(i,e),Ch.crossVectors(Ih,Pc);let o=this.direction.dot(Ch),a;if(o>0){if(r)return null;a=1}else if(o<0)a=-1,o=-o;else return null;ki.subVectors(this.origin,e);let c=a*this.direction.dot(Pc.crossVectors(ki,Pc));if(c<0)return null;let l=a*this.direction.dot(Ih.cross(ki));if(l<0||c+l>o)return null;let u=-a*ki.dot(Ch);return u<0?null:this.at(u/o,s)}applyMatrix4(e){return this.origin.applyMatrix4(e),this.direction.transformDirection(e),this}equals(e){return e.origin.equals(this.origin)&&e.direction.equals(this.direction)}clone(){return new this.constructor().copy(this)}},Ho=class extends vi{constructor(e){super(),this.isMeshBasicMaterial=!0,this.type="MeshBasicMaterial",this.color=new Ze(16777215),this.map=null,this.lightMap=null,this.lightMapIntensity=1,this.aoMap=null,this.aoMapIntensity=1,this.specularMap=null,this.alphaMap=null,this.envMap=null,this.envMapRotation=new Fo,this.combine=Wh,this.reflectivity=1,this.refractionRatio=.98,this.wireframe=!1,this.wireframeLinewidth=1,this.wireframeLinecap="round",this.wireframeLinejoin="round",this.fog=!0,this.setValues(e)}copy(e){return super.copy(e),this.color.copy(e.color),this.map=e.map,this.lightMap=e.lightMap,this.lightMapIntensity=e.lightMapIntensity,this.aoMap=e.aoMap,this.aoMapIntensity=e.aoMapIntensity,this.specularMap=e.specularMap,this.alphaMap=e.alphaMap,this.envMap=e.envMap,this.envMapRotation.copy(e.envMapRotation),this.combine=e.combine,this.reflectivity=e.reflectivity,this.refractionRatio=e.refractionRatio,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.wireframeLinecap=e.wireframeLinecap,this.wireframeLinejoin=e.wireframeLinejoin,this.fog=e.fog,this}},Ty=new Tt,Er=new ll,Lc=new Ds,wy=new V,Oc=new V,Fc=new V,kc=new V,Ah=new V,Uc=new V,Iy=new V,Bc=new V,Qt=class extends Rr{constructor(e=new Sn,t=new Ho){super(),this.isMesh=!0,this.type="Mesh",this.geometry=e,this.material=t,this.morphTargetDictionary=void 0,this.morphTargetInfluences=void 0,this.count=1,this.updateMorphTargets()}copy(e,t){return super.copy(e,t),e.morphTargetInfluences!==void 0&&(this.morphTargetInfluences=e.morphTargetInfluences.slice()),e.morphTargetDictionary!==void 0&&(this.morphTargetDictionary=Object.assign({},e.morphTargetDictionary)),this.material=Array.isArray(e.material)?e.material.slice():e.material,this.geometry=e.geometry,this}updateMorphTargets(){let t=this.geometry.morphAttributes,i=Object.keys(t);if(i.length>0){let r=t[i[0]];if(r!==void 0){this.morphTargetInfluences=[],this.morphTargetDictionary={};for(let s=0,o=r.length;s<o;s++){let a=r[s].name||String(s);this.morphTargetInfluences.push(0),this.morphTargetDictionary[a]=s}}}}getVertexPosition(e,t){let i=this.geometry,r=i.attributes.position,s=i.morphAttributes.position,o=i.morphTargetsRelative;t.fromBufferAttribute(r,e);let a=this.morphTargetInfluences;if(s&&a){Uc.set(0,0,0);for(let c=0,l=s.length;c<l;c++){let u=a[c],f=s[c];u!==0&&(Ah.fromBufferAttribute(f,e),o?Uc.addScaledVector(Ah,u):Uc.addScaledVector(Ah.sub(t),u))}t.add(Uc)}return t}raycast(e,t){let i=this.geometry,r=this.material,s=this.matrixWorld;r!==void 0&&(i.boundingSphere===null&&i.computeBoundingSphere(),Lc.copy(i.boundingSphere),Lc.applyMatrix4(s),Er.copy(e.ray).recast(e.near),!(Lc.containsPoint(Er.origin)===!1&&(Er.intersectSphere(Lc,wy)===null||Er.origin.distanceToSquared(wy)>(e.far-e.near)**2))&&(Ty.copy(s).invert(),Er.copy(e.ray).applyMatrix4(Ty),!(i.boundingBox!==null&&Er.intersectsBox(i.boundingBox)===!1)&&this._computeIntersections(e,t,Er)))}_computeIntersections(e,t,i){let r,s=this.geometry,o=this.material,a=s.index,c=s.attributes.position,l=s.attributes.uv,u=s.attributes.uv1,f=s.attributes.normal,d=s.groups,h=s.drawRange;if(a!==null)if(Array.isArray(o))for(let v=0,M=d.length;v<M;v++){let m=d[v],p=o[m.materialIndex],S=Math.max(m.start,h.start),T=Math.min(a.count,Math.min(m.start+m.count,h.start+h.count));for(let E=S,D=T;E<D;E+=3){let b=a.getX(E),A=a.getX(E+1),y=a.getX(E+2);r=Vc(this,p,e,i,l,u,f,b,A,y),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let v=Math.max(0,h.start),M=Math.min(a.count,h.start+h.count);for(let m=v,p=M;m<p;m+=3){let S=a.getX(m),T=a.getX(m+1),E=a.getX(m+2);r=Vc(this,o,e,i,l,u,f,S,T,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}else if(c!==void 0)if(Array.isArray(o))for(let v=0,M=d.length;v<M;v++){let m=d[v],p=o[m.materialIndex],S=Math.max(m.start,h.start),T=Math.min(c.count,Math.min(m.start+m.count,h.start+h.count));for(let E=S,D=T;E<D;E+=3){let b=E,A=E+1,y=E+2;r=Vc(this,p,e,i,l,u,f,b,A,y),r&&(r.faceIndex=Math.floor(E/3),r.face.materialIndex=m.materialIndex,t.push(r))}}else{let v=Math.max(0,h.start),M=Math.min(c.count,h.start+h.count);for(let m=v,p=M;m<p;m+=3){let S=m,T=m+1,E=m+2;r=Vc(this,o,e,i,l,u,f,S,T,E),r&&(r.faceIndex=Math.floor(m/3),t.push(r))}}}};function vb(n,e,t,i,r,s,o,a){let c;if(e.side===qt?c=i.intersectTriangle(o,s,r,!0,a):c=i.intersectTriangle(r,s,o,e.side===gi,a),c===null)return null;Bc.copy(a),Bc.applyMatrix4(n.matrixWorld);let l=t.ray.origin.distanceTo(Bc);return l<t.near||l>t.far?null:{distance:l,point:Bc.clone(),object:n}}function Vc(n,e,t,i,r,s,o,a,c,l){n.getVertexPosition(a,Oc),n.getVertexPosition(c,Fc),n.getVertexPosition(l,kc);let u=vb(n,e,t,i,Oc,Fc,kc,Iy);if(u){let f=new V;Bi.getBarycoord(Iy,Oc,Fc,kc,f),r&&(u.uv=Bi.getInterpolatedAttribute(r,a,c,l,f,new tt)),s&&(u.uv1=Bi.getInterpolatedAttribute(s,a,c,l,f,new tt)),o&&(u.normal=Bi.getInterpolatedAttribute(o,a,c,l,f,new V),u.normal.dot(i.direction)>0&&u.normal.multiplyScalar(-1));let d={a,b:c,c:l,normal:new V,materialIndex:0};Bi.getNormal(Oc,Fc,kc,d.normal),u.face=d,u.barycoord=f}return u}var ul=class extends xi{constructor(e=null,t=1,i=1,r,s,o,a,c,l=Pt,u=Pt,f,d){super(null,o,a,c,l,u,r,s,f,d),this.isDataTexture=!0,this.image={data:e,width:t,height:i},this.generateMipmaps=!1,this.flipY=!1,this.unpackAlignment=1}};var Dh=new V,yb=new V,_b=new Ae,Xn=class{constructor(e=new V(1,0,0),t=0){this.isPlane=!0,this.normal=e,this.constant=t}set(e,t){return this.normal.copy(e),this.constant=t,this}setComponents(e,t,i,r){return this.normal.set(e,t,i),this.constant=r,this}setFromNormalAndCoplanarPoint(e,t){return this.normal.copy(e),this.constant=-t.dot(this.normal),this}setFromCoplanarPoints(e,t,i){let r=Dh.subVectors(i,t).cross(yb.subVectors(e,t)).normalize();return this.setFromNormalAndCoplanarPoint(r,e),this}copy(e){return this.normal.copy(e.normal),this.constant=e.constant,this}normalize(){let e=1/this.normal.length();return this.normal.multiplyScalar(e),this.constant*=e,this}negate(){return this.constant*=-1,this.normal.negate(),this}distanceToPoint(e){return this.normal.dot(e)+this.constant}distanceToSphere(e){return this.distanceToPoint(e.center)-e.radius}projectPoint(e,t){return t.copy(e).addScaledVector(this.normal,-this.distanceToPoint(e))}intersectLine(e,t,i=!0){let r=e.delta(Dh),s=this.normal.dot(r);if(s===0)return this.distanceToPoint(e.start)===0?t.copy(e.start):null;let o=-(e.start.dot(this.normal)+this.constant)/s;return i===!0&&(o<0||o>1)?null:t.copy(e.start).addScaledVector(r,o)}intersectsLine(e){let t=this.distanceToPoint(e.start),i=this.distanceToPoint(e.end);return t<0&&i>0||i<0&&t>0}intersectsBox(e){return e.intersectsPlane(this)}intersectsSphere(e){return e.intersectsPlane(this)}coplanarPoint(e){return e.copy(this.normal).multiplyScalar(-this.constant)}applyMatrix4(e,t){let i=t||_b.getNormalMatrix(e),r=this.coplanarPoint(Dh).applyMatrix4(e),s=this.normal.applyMatrix3(i).normalize();return this.constant=-r.dot(s),this}translate(e){return this.constant-=e.dot(this.normal),this}equals(e){return e.normal.equals(this.normal)&&e.constant===this.constant}clone(){return new this.constructor().copy(this)}},br=new Ds,xb=new tt(.5,.5),Hc=new V,zo=class{constructor(e=new Xn,t=new Xn,i=new Xn,r=new Xn,s=new Xn,o=new Xn){this.planes=[e,t,i,r,s,o]}set(e,t,i,r,s,o){let a=this.planes;return a[0].copy(e),a[1].copy(t),a[2].copy(i),a[3].copy(r),a[4].copy(s),a[5].copy(o),this}copy(e){let t=this.planes;for(let i=0;i<6;i++)t[i].copy(e.planes[i]);return this}setFromProjectionMatrix(e,t=On,i=!1){let r=this.planes,s=e.elements,o=s[0],a=s[1],c=s[2],l=s[3],u=s[4],f=s[5],d=s[6],h=s[7],v=s[8],M=s[9],m=s[10],p=s[11],S=s[12],T=s[13],E=s[14],D=s[15];if(r[0].setComponents(l-o,h-u,p-v,D-S).normalize(),r[1].setComponents(l+o,h+u,p+v,D+S).normalize(),r[2].setComponents(l+a,h+f,p+M,D+T).normalize(),r[3].setComponents(l-a,h-f,p-M,D-T).normalize(),i)r[4].setComponents(c,d,m,E).normalize(),r[5].setComponents(l-c,h-d,p-m,D-E).normalize();else if(r[4].setComponents(l-c,h-d,p-m,D-E).normalize(),t===On)r[5].setComponents(l+c,h+d,p+m,D+E).normalize();else if(t===Po)r[5].setComponents(c,d,m,E).normalize();else throw new Error("THREE.Frustum.setFromProjectionMatrix(): Invalid coordinate system: "+t);return this}intersectsObject(e){if(e.boundingSphere!==void 0)e.boundingSphere===null&&e.computeBoundingSphere(),br.copy(e.boundingSphere).applyMatrix4(e.matrixWorld);else{let t=e.geometry;t.boundingSphere===null&&t.computeBoundingSphere(),br.copy(t.boundingSphere).applyMatrix4(e.matrixWorld)}return this.intersectsSphere(br)}intersectsSprite(e){br.center.set(0,0,0);let t=xb.distanceTo(e.center);return br.radius=.7071067811865476+t,br.applyMatrix4(e.matrixWorld),this.intersectsSphere(br)}intersectsSphere(e){let t=this.planes,i=e.center,r=-e.radius;for(let s=0;s<6;s++)if(t[s].distanceToPoint(i)<r)return!1;return!0}intersectsBox(e){let t=this.planes;for(let i=0;i<6;i++){let r=t[i];if(Hc.x=r.normal.x>0?e.max.x:e.min.x,Hc.y=r.normal.y>0?e.max.y:e.min.y,Hc.z=r.normal.z>0?e.max.z:e.min.z,r.distanceToPoint(Hc)<0)return!1}return!0}containsPoint(e){let t=this.planes;for(let i=0;i<6;i++)if(t[i].distanceToPoint(e)<0)return!1;return!0}clone(){return new this.constructor().copy(this)}};var Go=class extends xi{constructor(e=[],t=ji,i,r,s,o,a,c,l,u){super(e,t,i,r,s,o,a,c,l,u),this.isCubeTexture=!0,this.flipY=!1}get images(){return this.image}set images(e){this.image=e}};var yi=class extends xi{constructor(e,t,i=kn,r,s,o,a=Pt,c=Pt,l,u=Zn,f=1){if(u!==Zn&&u!==qi)throw new Error("DepthTexture format must be either THREE.DepthFormat or THREE.DepthStencilFormat");let d={width:e,height:t,depth:f};super(d,r,s,o,a,c,u,i,l),this.isDepthTexture=!0,this.flipY=!1,this.generateMipmaps=!1,this.compareFunction=null}copy(e){return super.copy(e),this.source=new Cs(Object.assign({},e.image)),this.compareFunction=e.compareFunction,this}toJSON(e){let t=super.toJSON(e);return this.compareFunction!==null&&(t.compareFunction=this.compareFunction),t}},dl=class extends yi{constructor(e,t=kn,i=ji,r,s,o=Pt,a=Pt,c,l=Zn){let u={width:e,height:e,depth:1},f=[u,u,u,u,u,u];super(e,e,t,i,r,s,o,a,c,l),this.image=f,this.isCubeDepthTexture=!0,this.isCubeTexture=!0}get images(){return this.image}set images(e){this.image=e}},Wo=class extends xi{constructor(e=null){super(),this.sourceTexture=e,this.isExternalTexture=!0}copy(e){return super.copy(e),this.sourceTexture=e.sourceTexture,this}},Rs=class n extends Sn{constructor(e=1,t=1,i=1,r=1,s=1,o=1){super(),this.type="BoxGeometry",this.parameters={width:e,height:t,depth:i,widthSegments:r,heightSegments:s,depthSegments:o};let a=this;r=Math.floor(r),s=Math.floor(s),o=Math.floor(o);let c=[],l=[],u=[],f=[],d=0,h=0;v("z","y","x",-1,-1,i,t,e,o,s,0),v("z","y","x",1,-1,i,t,-e,o,s,1),v("x","z","y",1,1,e,i,t,r,o,2),v("x","z","y",1,-1,e,i,-t,r,o,3),v("x","y","z",1,-1,e,t,i,r,s,4),v("x","y","z",-1,-1,e,t,-i,r,s,5),this.setIndex(c),this.setAttribute("position",new Mn(l,3)),this.setAttribute("normal",new Mn(u,3)),this.setAttribute("uv",new Mn(f,2));function v(M,m,p,S,T,E,D,b,A,y,w){let L=E/A,I=D/y,k=E/2,W=D/2,X=b/2,P=A+1,z=y+1,F=0,K=0,Q=new V;for(let le=0;le<z;le++){let ye=le*I-W;for(let Se=0;Se<P;Se++){let qe=Se*L-k;Q[M]=qe*S,Q[m]=ye*T,Q[p]=X,l.push(Q.x,Q.y,Q.z),Q[M]=0,Q[m]=0,Q[p]=b>0?1:-1,u.push(Q.x,Q.y,Q.z),f.push(Se/A),f.push(1-le/y),F+=1}}for(let le=0;le<y;le++)for(let ye=0;ye<A;ye++){let Se=d+ye+P*le,qe=d+ye+P*(le+1),nt=d+(ye+1)+P*(le+1),Oe=d+(ye+1)+P*le;c.push(Se,qe,Oe),c.push(qe,nt,Oe),K+=6}a.addGroup(h,K,w),h+=K,d+=F}}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.depth,e.widthSegments,e.heightSegments,e.depthSegments)}};var jo=class n extends Sn{constructor(e=1,t=1,i=1,r=1){super(),this.type="PlaneGeometry",this.parameters={width:e,height:t,widthSegments:i,heightSegments:r};let s=e/2,o=t/2,a=Math.floor(i),c=Math.floor(r),l=a+1,u=c+1,f=e/a,d=t/c,h=[],v=[],M=[],m=[];for(let p=0;p<u;p++){let S=p*d-o;for(let T=0;T<l;T++){let E=T*f-s;v.push(E,-S,0),M.push(0,0,1),m.push(T/a),m.push(1-p/c)}}for(let p=0;p<c;p++)for(let S=0;S<a;S++){let T=S+l*p,E=S+l*(p+1),D=S+1+l*(p+1),b=S+1+l*p;h.push(T,E,b),h.push(E,D,b)}this.setIndex(h),this.setAttribute("position",new Mn(v,3)),this.setAttribute("normal",new Mn(M,3)),this.setAttribute("uv",new Mn(m,2))}copy(e){return super.copy(e),this.parameters=Object.assign({},e.parameters),this}static fromJSON(e){return new n(e.width,e.height,e.widthSegments,e.heightSegments)}};function Nr(n){let e={};for(let t in n){e[t]={};for(let i in n[t]){let r=n[t][i];if(Cy(r))r.isRenderTargetTexture?(be("UniformsUtils: Textures of render targets cannot be cloned via cloneUniforms() or mergeUniforms()."),e[t][i]=null):e[t][i]=r.clone();else if(Array.isArray(r))if(Cy(r[0])){let s=[];for(let o=0,a=r.length;o<a;o++)s[o]=r[o].clone();e[t][i]=s}else e[t][i]=r.slice();else e[t][i]=r}}return e}function zt(n){let e={};for(let t=0;t<n.length;t++){let i=Nr(n[t]);for(let r in i)e[r]=i[r]}return e}function Cy(n){return n&&(n.isColor||n.isMatrix3||n.isMatrix4||n.isVector2||n.isVector3||n.isVector4||n.isTexture||n.isQuaternion)}function Mb(n){let e=[];for(let t=0;t<n.length;t++)e.push(n[t].clone());return e}function cp(n){let e=n.getRenderTarget();return e===null?n.outputColorSpace:e.isXRRenderTarget===!0?e.texture.colorSpace:ze.workingColorSpace}var p_={clone:Nr,merge:zt},Sb=`void main() {
	gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
}`,Eb=`void main() {
	gl_FragColor = vec4( 1.0, 0.0, 0.0, 1.0 );
}`,fn=class extends vi{constructor(e){super(),this.isShaderMaterial=!0,this.type="ShaderMaterial",this.defines={},this.uniforms={},this.uniformsGroups=[],this.vertexShader=Sb,this.fragmentShader=Eb,this.linewidth=1,this.wireframe=!1,this.wireframeLinewidth=1,this.fog=!1,this.lights=!1,this.clipping=!1,this.forceSinglePass=!0,this.extensions={clipCullDistance:!1,multiDraw:!1},this.defaultAttributeValues={color:[1,1,1],uv:[0,0],uv1:[0,0]},this.index0AttributeName=void 0,this.uniformsNeedUpdate=!1,this.glslVersion=null,e!==void 0&&this.setValues(e)}copy(e){return super.copy(e),this.fragmentShader=e.fragmentShader,this.vertexShader=e.vertexShader,this.uniforms=Nr(e.uniforms),this.uniformsGroups=Mb(e.uniformsGroups),this.defines=Object.assign({},e.defines),this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this.fog=e.fog,this.lights=e.lights,this.clipping=e.clipping,this.extensions=Object.assign({},e.extensions),this.glslVersion=e.glslVersion,this.defaultAttributeValues=Object.assign({},e.defaultAttributeValues),this.index0AttributeName=e.index0AttributeName,this.uniformsNeedUpdate=e.uniformsNeedUpdate,this}toJSON(e){let t=super.toJSON(e);t.glslVersion=this.glslVersion,t.uniforms={};for(let r in this.uniforms){let o=this.uniforms[r].value;o&&o.isTexture?t.uniforms[r]={type:"t",value:o.toJSON(e).uuid}:o&&o.isColor?t.uniforms[r]={type:"c",value:o.getHex()}:o&&o.isVector2?t.uniforms[r]={type:"v2",value:o.toArray()}:o&&o.isVector3?t.uniforms[r]={type:"v3",value:o.toArray()}:o&&o.isVector4?t.uniforms[r]={type:"v4",value:o.toArray()}:o&&o.isMatrix3?t.uniforms[r]={type:"m3",value:o.toArray()}:o&&o.isMatrix4?t.uniforms[r]={type:"m4",value:o.toArray()}:t.uniforms[r]={value:o}}Object.keys(this.defines).length>0&&(t.defines=this.defines),t.vertexShader=this.vertexShader,t.fragmentShader=this.fragmentShader,t.lights=this.lights,t.clipping=this.clipping;let i={};for(let r in this.extensions)this.extensions[r]===!0&&(i[r]=!0);return Object.keys(i).length>0&&(t.extensions=i),t}},Ns=class extends fn{constructor(e){super(e),this.isRawShaderMaterial=!0,this.type="RawShaderMaterial"}};var fl=class extends vi{constructor(e){super(),this.isMeshDepthMaterial=!0,this.type="MeshDepthMaterial",this.depthPacking=t_,this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.wireframe=!1,this.wireframeLinewidth=1,this.setValues(e)}copy(e){return super.copy(e),this.depthPacking=e.depthPacking,this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this.wireframe=e.wireframe,this.wireframeLinewidth=e.wireframeLinewidth,this}},hl=class extends vi{constructor(e){super(),this.isMeshDistanceMaterial=!0,this.type="MeshDistanceMaterial",this.map=null,this.alphaMap=null,this.displacementMap=null,this.displacementScale=1,this.displacementBias=0,this.setValues(e)}copy(e){return super.copy(e),this.map=e.map,this.alphaMap=e.alphaMap,this.displacementMap=e.displacementMap,this.displacementScale=e.displacementScale,this.displacementBias=e.displacementBias,this}};function zc(n,e){return!n||n.constructor===e?n:typeof e.BYTES_PER_ELEMENT=="number"?new e(n):Array.prototype.slice.call(n)}var zi=class{constructor(e,t,i,r){this.parameterPositions=e,this._cachedIndex=0,this.resultBuffer=r!==void 0?r:new t.constructor(i),this.sampleValues=t,this.valueSize=i,this.settings=null,this.DefaultSettings_={}}evaluate(e){let t=this.parameterPositions,i=this._cachedIndex,r=t[i],s=t[i-1];e:{t:{let o;n:{i:if(!(e<r)){for(let a=i+2;;){if(r===void 0){if(e<s)break i;return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}if(i===a)break;if(s=r,r=t[++i],e<r)break t}o=t.length;break n}if(!(e>=s)){let a=t[1];e<a&&(i=2,s=a);for(let c=i-2;;){if(s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(i===c)break;if(r=s,s=t[--i-1],e>=s)break t}o=i,i=0;break n}break e}for(;i<o;){let a=i+o>>>1;e<t[a]?o=a:i=a+1}if(r=t[i],s=t[i-1],s===void 0)return this._cachedIndex=0,this.copySampleValue_(0);if(r===void 0)return i=t.length,this._cachedIndex=i,this.copySampleValue_(i-1)}this._cachedIndex=i,this.intervalChanged_(i,s,r)}return this.interpolate_(i,s,e,r)}getSettings_(){return this.settings||this.DefaultSettings_}copySampleValue_(e){let t=this.resultBuffer,i=this.sampleValues,r=this.valueSize,s=e*r;for(let o=0;o!==r;++o)t[o]=i[s+o];return t}interpolate_(){throw new Error("call to abstract method")}intervalChanged_(){}},pl=class extends zi{constructor(e,t,i,r){super(e,t,i,r),this._weightPrev=-0,this._offsetPrev=-0,this._weightNext=-0,this._offsetNext=-0,this.DefaultSettings_={endingStart:Ph,endingEnd:Ph}}intervalChanged_(e,t,i){let r=this.parameterPositions,s=e-2,o=e+1,a=r[s],c=r[o];if(a===void 0)switch(this.getSettings_().endingStart){case Lh:s=e,a=2*t-i;break;case Oh:s=r.length-2,a=t+r[s]-r[s+1];break;default:s=e,a=i}if(c===void 0)switch(this.getSettings_().endingEnd){case Lh:o=e,c=2*i-t;break;case Oh:o=1,c=i+r[1]-r[0];break;default:o=e-1,c=t}let l=(i-t)*.5,u=this.valueSize;this._weightPrev=l/(t-a),this._weightNext=l/(c-i),this._offsetPrev=s*u,this._offsetNext=o*u}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this._offsetPrev,f=this._offsetNext,d=this._weightPrev,h=this._weightNext,v=(i-t)/(r-t),M=v*v,m=M*v,p=-d*m+2*d*M-d*v,S=(1+d)*m+(-1.5-2*d)*M+(-.5+d)*v+1,T=(-1-h)*m+(1.5+h)*M+.5*v,E=h*m-h*M;for(let D=0;D!==a;++D)s[D]=p*o[u+D]+S*o[l+D]+T*o[c+D]+E*o[f+D];return s}},ml=class extends zi{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=(i-t)/(r-t),f=1-u;for(let d=0;d!==a;++d)s[d]=o[l+d]*f+o[c+d]*u;return s}},gl=class extends zi{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e){return this.copySampleValue_(e-1)}},vl=class extends zi{interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=e*a,l=c-a,u=this.settings||this.DefaultSettings_,f=u.inTangents,d=u.outTangents;if(!f||!d){let M=(i-t)/(r-t),m=1-M;for(let p=0;p!==a;++p)s[p]=o[l+p]*m+o[c+p]*M;return s}let h=a*2,v=e-1;for(let M=0;M!==a;++M){let m=o[l+M],p=o[c+M],S=v*h+M*2,T=d[S],E=d[S+1],D=e*h+M*2,b=f[D],A=f[D+1],y=(i-t)/(r-t),w,L,I,k,W;for(let X=0;X<8;X++){w=y*y,L=w*y,I=1-y,k=I*I,W=k*I;let z=W*t+3*k*y*T+3*I*w*b+L*r-i;if(Math.abs(z)<1e-10)break;let F=3*k*(T-t)+6*I*y*(b-T)+3*w*(r-b);if(Math.abs(F)<1e-10)break;y=y-z/F,y=Math.max(0,Math.min(1,y))}s[M]=W*m+3*k*y*E+3*I*w*A+L*p}return s}},hn=class{constructor(e,t,i,r){if(e===void 0)throw new Error("THREE.KeyframeTrack: track name is undefined");if(t===void 0||t.length===0)throw new Error("THREE.KeyframeTrack: no keyframes in track named "+e);this.name=e,this.times=zc(t,this.TimeBufferType),this.values=zc(i,this.ValueBufferType),this.setInterpolation(r||this.DefaultInterpolation)}static toJSON(e){let t=e.constructor,i;if(t.toJSON!==this.toJSON)i=t.toJSON(e);else{i={name:e.name,times:zc(e.times,Array),values:zc(e.values,Array)};let r=e.getInterpolation();r!==e.DefaultInterpolation&&(i.interpolation=r)}return i.type=e.ValueTypeName,i}InterpolantFactoryMethodDiscrete(e){return new gl(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodLinear(e){return new ml(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodSmooth(e){return new pl(this.times,this.values,this.getValueSize(),e)}InterpolantFactoryMethodBezier(e){let t=new vl(this.times,this.values,this.getValueSize(),e);return this.settings&&(t.settings=this.settings),t}setInterpolation(e){let t;switch(e){case Do:t=this.InterpolantFactoryMethodDiscrete;break;case il:t=this.InterpolantFactoryMethodLinear;break;case jc:t=this.InterpolantFactoryMethodSmooth;break;case Nh:t=this.InterpolantFactoryMethodBezier;break}if(t===void 0){let i="unsupported interpolation for "+this.ValueTypeName+" keyframe track named "+this.name;if(this.createInterpolant===void 0)if(e!==this.DefaultInterpolation)this.setInterpolation(this.DefaultInterpolation);else throw new Error(i);return be("KeyframeTrack:",i),this}return this.createInterpolant=t,this}getInterpolation(){switch(this.createInterpolant){case this.InterpolantFactoryMethodDiscrete:return Do;case this.InterpolantFactoryMethodLinear:return il;case this.InterpolantFactoryMethodSmooth:return jc;case this.InterpolantFactoryMethodBezier:return Nh}}getValueSize(){return this.values.length/this.times.length}shift(e){if(e!==0){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]+=e}return this}scale(e){if(e!==1){let t=this.times;for(let i=0,r=t.length;i!==r;++i)t[i]*=e}return this}trim(e,t){let i=this.times,r=i.length,s=0,o=r-1;for(;s!==r&&i[s]<e;)++s;for(;o!==-1&&i[o]>t;)--o;if(++o,s!==0||o!==r){s>=o&&(o=Math.max(o,1),s=o-1);let a=this.getValueSize();this.times=i.slice(s,o),this.values=this.values.slice(s*a,o*a)}return this}validate(){let e=!0,t=this.getValueSize();t-Math.floor(t)!==0&&(we("KeyframeTrack: Invalid value size in track.",this),e=!1);let i=this.times,r=this.values,s=i.length;s===0&&(we("KeyframeTrack: Track is empty.",this),e=!1);let o=null;for(let a=0;a!==s;a++){let c=i[a];if(typeof c=="number"&&isNaN(c)){we("KeyframeTrack: Time is not a valid number.",this,a,c),e=!1;break}if(o!==null&&o>c){we("KeyframeTrack: Out of order keys.",this,a,c,o),e=!1;break}o=c}if(r!==void 0&&tb(r))for(let a=0,c=r.length;a!==c;++a){let l=r[a];if(isNaN(l)){we("KeyframeTrack: Value is not a valid number.",this,a,l),e=!1;break}}return e}optimize(){let e=this.times.slice(),t=this.values.slice(),i=this.getValueSize(),r=this.getInterpolation()===jc,s=e.length-1,o=1;for(let a=1;a<s;++a){let c=!1,l=e[a],u=e[a+1];if(l!==u&&(a!==1||l!==e[0]))if(r)c=!0;else{let f=a*i,d=f-i,h=f+i;for(let v=0;v!==i;++v){let M=t[f+v];if(M!==t[d+v]||M!==t[h+v]){c=!0;break}}}if(c){if(a!==o){e[o]=e[a];let f=a*i,d=o*i;for(let h=0;h!==i;++h)t[d+h]=t[f+h]}++o}}if(s>0){e[o]=e[s];for(let a=s*i,c=o*i,l=0;l!==i;++l)t[c+l]=t[a+l];++o}return o!==e.length?(this.times=e.slice(0,o),this.values=t.slice(0,o*i)):(this.times=e,this.values=t),this}clone(){let e=this.times.slice(),t=this.values.slice(),i=this.constructor,r=new i(this.name,e,t);return r.createInterpolant=this.createInterpolant,r}};hn.prototype.ValueTypeName="";hn.prototype.TimeBufferType=Float32Array;hn.prototype.ValueBufferType=Float32Array;hn.prototype.DefaultInterpolation=il;var Gi=class extends hn{constructor(e,t,i){super(e,t,i)}};Gi.prototype.ValueTypeName="bool";Gi.prototype.ValueBufferType=Array;Gi.prototype.DefaultInterpolation=Do;Gi.prototype.InterpolantFactoryMethodLinear=void 0;Gi.prototype.InterpolantFactoryMethodSmooth=void 0;var yl=class extends hn{constructor(e,t,i,r){super(e,t,i,r)}};yl.prototype.ValueTypeName="color";var _l=class extends hn{constructor(e,t,i,r){super(e,t,i,r)}};_l.prototype.ValueTypeName="number";var xl=class extends zi{constructor(e,t,i,r){super(e,t,i,r)}interpolate_(e,t,i,r){let s=this.resultBuffer,o=this.sampleValues,a=this.valueSize,c=(i-t)/(r-t),l=e*a;for(let u=l+a;l!==u;l+=4)Kn.slerpFlat(s,0,o,l-a,o,l,c);return s}},$o=class extends hn{constructor(e,t,i,r){super(e,t,i,r)}InterpolantFactoryMethodLinear(e){return new xl(this.times,this.values,this.getValueSize(),e)}};$o.prototype.ValueTypeName="quaternion";$o.prototype.InterpolantFactoryMethodSmooth=void 0;var Wi=class extends hn{constructor(e,t,i){super(e,t,i)}};Wi.prototype.ValueTypeName="string";Wi.prototype.ValueBufferType=Array;Wi.prototype.DefaultInterpolation=Do;Wi.prototype.InterpolantFactoryMethodLinear=void 0;Wi.prototype.InterpolantFactoryMethodSmooth=void 0;var Ml=class extends hn{constructor(e,t,i,r){super(e,t,i,r)}};Ml.prototype.ValueTypeName="vector";var Gc=new V,Wc=new Kn,qn=new V,qo=class extends Rr{constructor(){super(),this.isCamera=!0,this.type="Camera",this.matrixWorldInverse=new Tt,this.projectionMatrix=new Tt,this.projectionMatrixInverse=new Tt,this.coordinateSystem=On,this._reversedDepth=!1}get reversedDepth(){return this._reversedDepth}copy(e,t){return super.copy(e,t),this.matrixWorldInverse.copy(e.matrixWorldInverse),this.projectionMatrix.copy(e.projectionMatrix),this.projectionMatrixInverse.copy(e.projectionMatrixInverse),this.coordinateSystem=e.coordinateSystem,this}getWorldDirection(e){return super.getWorldDirection(e).negate()}updateMatrixWorld(e){super.updateMatrixWorld(e),this.matrixWorld.decompose(Gc,Wc,qn),qn.x===1&&qn.y===1&&qn.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Gc,Wc,qn.set(1,1,1)).invert()}updateWorldMatrix(e,t){super.updateWorldMatrix(e,t),this.matrixWorld.decompose(Gc,Wc,qn),qn.x===1&&qn.y===1&&qn.z===1?this.matrixWorldInverse.copy(this.matrixWorld).invert():this.matrixWorldInverse.compose(Gc,Wc,qn.set(1,1,1)).invert()}clone(){return new this.constructor().copy(this)}},Ui=new V,Ay=new tt,Dy=new tt,Kt=class extends qo{constructor(e=50,t=1,i=.1,r=2e3){super(),this.isPerspectiveCamera=!0,this.type="PerspectiveCamera",this.fov=e,this.zoom=1,this.near=i,this.far=r,this.focus=10,this.aspect=t,this.view=null,this.filmGauge=35,this.filmOffset=0,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.fov=e.fov,this.zoom=e.zoom,this.near=e.near,this.far=e.far,this.focus=e.focus,this.aspect=e.aspect,this.view=e.view===null?null:Object.assign({},e.view),this.filmGauge=e.filmGauge,this.filmOffset=e.filmOffset,this}setFocalLength(e){let t=.5*this.getFilmHeight()/e;this.fov=sl*2*Math.atan(t),this.updateProjectionMatrix()}getFocalLength(){let e=Math.tan(ch*.5*this.fov);return .5*this.getFilmHeight()/e}getEffectiveFOV(){return sl*2*Math.atan(Math.tan(ch*.5*this.fov)/this.zoom)}getFilmWidth(){return this.filmGauge*Math.min(this.aspect,1)}getFilmHeight(){return this.filmGauge/Math.max(this.aspect,1)}getViewBounds(e,t,i){Ui.set(-1,-1,.5).applyMatrix4(this.projectionMatrixInverse),t.set(Ui.x,Ui.y).multiplyScalar(-e/Ui.z),Ui.set(1,1,.5).applyMatrix4(this.projectionMatrixInverse),i.set(Ui.x,Ui.y).multiplyScalar(-e/Ui.z)}getViewSize(e,t){return this.getViewBounds(e,Ay,Dy),t.subVectors(Dy,Ay)}setViewOffset(e,t,i,r,s,o){this.aspect=e/t,this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=this.near,t=e*Math.tan(ch*.5*this.fov)/this.zoom,i=2*t,r=this.aspect*i,s=-.5*r,o=this.view;if(this.view!==null&&this.view.enabled){let c=o.fullWidth,l=o.fullHeight;s+=o.offsetX*r/c,t-=o.offsetY*i/l,r*=o.width/c,i*=o.height/l}let a=this.filmOffset;a!==0&&(s+=e*a/this.getFilmWidth()),this.projectionMatrix.makePerspective(s,s+r,t,t-i,e,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.fov=this.fov,t.object.zoom=this.zoom,t.object.near=this.near,t.object.far=this.far,t.object.focus=this.focus,t.object.aspect=this.aspect,this.view!==null&&(t.object.view=Object.assign({},this.view)),t.object.filmGauge=this.filmGauge,t.object.filmOffset=this.filmOffset,t}};var Ar=class extends qo{constructor(e=-1,t=1,i=1,r=-1,s=.1,o=2e3){super(),this.isOrthographicCamera=!0,this.type="OrthographicCamera",this.zoom=1,this.view=null,this.left=e,this.right=t,this.top=i,this.bottom=r,this.near=s,this.far=o,this.updateProjectionMatrix()}copy(e,t){return super.copy(e,t),this.left=e.left,this.right=e.right,this.top=e.top,this.bottom=e.bottom,this.near=e.near,this.far=e.far,this.zoom=e.zoom,this.view=e.view===null?null:Object.assign({},e.view),this}setViewOffset(e,t,i,r,s,o){this.view===null&&(this.view={enabled:!0,fullWidth:1,fullHeight:1,offsetX:0,offsetY:0,width:1,height:1}),this.view.enabled=!0,this.view.fullWidth=e,this.view.fullHeight=t,this.view.offsetX=i,this.view.offsetY=r,this.view.width=s,this.view.height=o,this.updateProjectionMatrix()}clearViewOffset(){this.view!==null&&(this.view.enabled=!1),this.updateProjectionMatrix()}updateProjectionMatrix(){let e=(this.right-this.left)/(2*this.zoom),t=(this.top-this.bottom)/(2*this.zoom),i=(this.right+this.left)/2,r=(this.top+this.bottom)/2,s=i-e,o=i+e,a=r+t,c=r-t;if(this.view!==null&&this.view.enabled){let l=(this.right-this.left)/this.view.fullWidth/this.zoom,u=(this.top-this.bottom)/this.view.fullHeight/this.zoom;s+=l*this.view.offsetX,o=s+l*this.view.width,a-=u*this.view.offsetY,c=a-u*this.view.height}this.projectionMatrix.makeOrthographic(s,o,a,c,this.near,this.far,this.coordinateSystem,this.reversedDepth),this.projectionMatrixInverse.copy(this.projectionMatrix).invert()}toJSON(e){let t=super.toJSON(e);return t.object.zoom=this.zoom,t.object.left=this.left,t.object.right=this.right,t.object.top=this.top,t.object.bottom=this.bottom,t.object.near=this.near,t.object.far=this.far,this.view!==null&&(t.object.view=Object.assign({},this.view)),t}};var bs=-90,Ts=1,Sl=class extends Rr{constructor(e,t,i){super(),this.type="CubeCamera",this.renderTarget=i,this.coordinateSystem=null,this.activeMipmapLevel=0;let r=new Kt(bs,Ts,e,t);r.layers=this.layers,this.add(r);let s=new Kt(bs,Ts,e,t);s.layers=this.layers,this.add(s);let o=new Kt(bs,Ts,e,t);o.layers=this.layers,this.add(o);let a=new Kt(bs,Ts,e,t);a.layers=this.layers,this.add(a);let c=new Kt(bs,Ts,e,t);c.layers=this.layers,this.add(c);let l=new Kt(bs,Ts,e,t);l.layers=this.layers,this.add(l)}updateCoordinateSystem(){let e=this.coordinateSystem,t=this.children.concat(),[i,r,s,o,a,c]=t;for(let l of t)this.remove(l);if(e===On)i.up.set(0,1,0),i.lookAt(1,0,0),r.up.set(0,1,0),r.lookAt(-1,0,0),s.up.set(0,0,-1),s.lookAt(0,1,0),o.up.set(0,0,1),o.lookAt(0,-1,0),a.up.set(0,1,0),a.lookAt(0,0,1),c.up.set(0,1,0),c.lookAt(0,0,-1);else if(e===Po)i.up.set(0,-1,0),i.lookAt(-1,0,0),r.up.set(0,-1,0),r.lookAt(1,0,0),s.up.set(0,0,1),s.lookAt(0,1,0),o.up.set(0,0,-1),o.lookAt(0,-1,0),a.up.set(0,-1,0),a.lookAt(0,0,1),c.up.set(0,-1,0),c.lookAt(0,0,-1);else throw new Error("THREE.CubeCamera.updateCoordinateSystem(): Invalid coordinate system: "+e);for(let l of t)this.add(l),l.updateMatrixWorld()}update(e,t){this.parent===null&&this.updateMatrixWorld();let{renderTarget:i,activeMipmapLevel:r}=this;this.coordinateSystem!==e.coordinateSystem&&(this.coordinateSystem=e.coordinateSystem,this.updateCoordinateSystem());let[s,o,a,c,l,u]=this.children,f=e.getRenderTarget(),d=e.getActiveCubeFace(),h=e.getActiveMipmapLevel(),v=e.xr.enabled;e.xr.enabled=!1;let M=i.texture.generateMipmaps;i.texture.generateMipmaps=!1;let m=!1;e.isWebGLRenderer===!0?m=e.state.buffers.depth.getReversed():m=e.reversedDepthBuffer,e.setRenderTarget(i,0,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,s),e.setRenderTarget(i,1,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,o),e.setRenderTarget(i,2,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,a),e.setRenderTarget(i,3,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,c),e.setRenderTarget(i,4,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,l),i.texture.generateMipmaps=M,e.setRenderTarget(i,5,r),m&&e.autoClear===!1&&e.clearDepth(),e.render(t,u),e.setRenderTarget(f,d,h),e.xr.enabled=v,i.texture.needsPMREMUpdate=!0}},El=class extends Kt{constructor(e=[]){super(),this.isArrayCamera=!0,this.isMultiViewCamera=!1,this.cameras=e}};var lp="\\[\\]\\.:\\/",bb=new RegExp("["+lp+"]","g"),up="[^"+lp+"]",Tb="[^"+lp.replace("\\.","")+"]",wb=/((?:WC+[\/:])*)/.source.replace("WC",up),Ib=/(WCOD+)?/.source.replace("WCOD",Tb),Cb=/(?:\.(WC+)(?:\[(.+)\])?)?/.source.replace("WC",up),Ab=/\.(WC+)(?:\[(.+)\])?/.source.replace("WC",up),Db=new RegExp("^"+wb+Ib+Cb+Ab+"$"),Rb=["material","materials","bones","map"],Uh=class{constructor(e,t,i){let r=i||yt.parseTrackName(t);this._targetGroup=e,this._bindings=e.subscribe_(t,r)}getValue(e,t){this.bind();let i=this._targetGroup.nCachedObjects_,r=this._bindings[i];r!==void 0&&r.getValue(e,t)}setValue(e,t){let i=this._bindings;for(let r=this._targetGroup.nCachedObjects_,s=i.length;r!==s;++r)i[r].setValue(e,t)}bind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].bind()}unbind(){let e=this._bindings;for(let t=this._targetGroup.nCachedObjects_,i=e.length;t!==i;++t)e[t].unbind()}},yt=(()=>{class n{constructor(t,i,r){this.path=i,this.parsedPath=r||n.parseTrackName(i),this.node=n.findNode(t,this.parsedPath.nodeName),this.rootNode=t,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}static create(t,i,r){return t&&t.isAnimationObjectGroup?new n.Composite(t,i,r):new n(t,i,r)}static sanitizeNodeName(t){return t.replace(/\s/g,"_").replace(bb,"")}static parseTrackName(t){let i=Db.exec(t);if(i===null)throw new Error("PropertyBinding: Cannot parse trackName: "+t);let r={nodeName:i[2],objectName:i[3],objectIndex:i[4],propertyName:i[5],propertyIndex:i[6]},s=r.nodeName&&r.nodeName.lastIndexOf(".");if(s!==void 0&&s!==-1){let o=r.nodeName.substring(s+1);Rb.indexOf(o)!==-1&&(r.nodeName=r.nodeName.substring(0,s),r.objectName=o)}if(r.propertyName===null||r.propertyName.length===0)throw new Error("PropertyBinding: can not parse propertyName from trackName: "+t);return r}static findNode(t,i){if(i===void 0||i===""||i==="."||i===-1||i===t.name||i===t.uuid)return t;if(t.skeleton){let r=t.skeleton.getBoneByName(i);if(r!==void 0)return r}if(t.children){let r=function(o){for(let a=0;a<o.length;a++){let c=o[a];if(c.name===i||c.uuid===i)return c;let l=r(c.children);if(l)return l}return null},s=r(t.children);if(s)return s}return null}_getValue_unavailable(){}_setValue_unavailable(){}_getValue_direct(t,i){t[i]=this.targetObject[this.propertyName]}_getValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)t[i++]=r[s]}_getValue_arrayElement(t,i){t[i]=this.resolvedProperty[this.propertyIndex]}_getValue_toArray(t,i){this.resolvedProperty.toArray(t,i)}_setValue_direct(t,i){this.targetObject[this.propertyName]=t[i]}_setValue_direct_setNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.needsUpdate=!0}_setValue_direct_setMatrixWorldNeedsUpdate(t,i){this.targetObject[this.propertyName]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_array(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++]}_setValue_array_setNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.needsUpdate=!0}_setValue_array_setMatrixWorldNeedsUpdate(t,i){let r=this.resolvedProperty;for(let s=0,o=r.length;s!==o;++s)r[s]=t[i++];this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_arrayElement(t,i){this.resolvedProperty[this.propertyIndex]=t[i]}_setValue_arrayElement_setNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.needsUpdate=!0}_setValue_arrayElement_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty[this.propertyIndex]=t[i],this.targetObject.matrixWorldNeedsUpdate=!0}_setValue_fromArray(t,i){this.resolvedProperty.fromArray(t,i)}_setValue_fromArray_setNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.needsUpdate=!0}_setValue_fromArray_setMatrixWorldNeedsUpdate(t,i){this.resolvedProperty.fromArray(t,i),this.targetObject.matrixWorldNeedsUpdate=!0}_getValue_unbound(t,i){this.bind(),this.getValue(t,i)}_setValue_unbound(t,i){this.bind(),this.setValue(t,i)}bind(){let t=this.node,i=this.parsedPath,r=i.objectName,s=i.propertyName,o=i.propertyIndex;if(t||(t=n.findNode(this.rootNode,i.nodeName),this.node=t),this.getValue=this._getValue_unavailable,this.setValue=this._setValue_unavailable,!t){be("PropertyBinding: No target node found for track: "+this.path+".");return}if(r){let u=i.objectIndex;switch(r){case"materials":if(!t.material){we("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.materials){we("PropertyBinding: Can not bind to material.materials as node.material does not have a materials array.",this);return}t=t.material.materials;break;case"bones":if(!t.skeleton){we("PropertyBinding: Can not bind to bones as node does not have a skeleton.",this);return}t=t.skeleton.bones;for(let f=0;f<t.length;f++)if(t[f].name===u){u=f;break}break;case"map":if("map"in t){t=t.map;break}if(!t.material){we("PropertyBinding: Can not bind to material as node does not have a material.",this);return}if(!t.material.map){we("PropertyBinding: Can not bind to material.map as node.material does not have a map.",this);return}t=t.material.map;break;default:if(t[r]===void 0){we("PropertyBinding: Can not bind to objectName of node undefined.",this);return}t=t[r]}if(u!==void 0){if(t[u]===void 0){we("PropertyBinding: Trying to bind to objectIndex of objectName, but is undefined.",this,t);return}t=t[u]}}let a=t[s];if(a===void 0){let u=i.nodeName;we("PropertyBinding: Trying to update property for track: "+u+"."+s+" but it wasn't found.",t);return}let c=this.Versioning.None;this.targetObject=t,t.isMaterial===!0?c=this.Versioning.NeedsUpdate:t.isObject3D===!0&&(c=this.Versioning.MatrixWorldNeedsUpdate);let l=this.BindingType.Direct;if(o!==void 0){if(s==="morphTargetInfluences"){if(!t.geometry){we("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.",this);return}if(!t.geometry.morphAttributes){we("PropertyBinding: Can not bind to morphTargetInfluences because node does not have a geometry.morphAttributes.",this);return}t.morphTargetDictionary[o]!==void 0&&(o=t.morphTargetDictionary[o])}l=this.BindingType.ArrayElement,this.resolvedProperty=a,this.propertyIndex=o}else a.fromArray!==void 0&&a.toArray!==void 0?(l=this.BindingType.HasFromToArray,this.resolvedProperty=a):Array.isArray(a)?(l=this.BindingType.EntireArray,this.resolvedProperty=a):this.propertyName=s;this.getValue=this.GetterByBindingType[l],this.setValue=this.SetterByBindingTypeAndVersioning[l][c]}unbind(){this.node=null,this.getValue=this._getValue_unbound,this.setValue=this._setValue_unbound}}return n.Composite=Uh,n})();yt.prototype.BindingType={Direct:0,EntireArray:1,ArrayElement:2,HasFromToArray:3};yt.prototype.Versioning={None:0,NeedsUpdate:1,MatrixWorldNeedsUpdate:2};yt.prototype.GetterByBindingType=[yt.prototype._getValue_direct,yt.prototype._getValue_array,yt.prototype._getValue_arrayElement,yt.prototype._getValue_toArray];yt.prototype.SetterByBindingTypeAndVersioning=[[yt.prototype._setValue_direct,yt.prototype._setValue_direct_setNeedsUpdate,yt.prototype._setValue_direct_setMatrixWorldNeedsUpdate],[yt.prototype._setValue_array,yt.prototype._setValue_array_setNeedsUpdate,yt.prototype._setValue_array_setMatrixWorldNeedsUpdate],[yt.prototype._setValue_arrayElement,yt.prototype._setValue_arrayElement_setNeedsUpdate,yt.prototype._setValue_arrayElement_setMatrixWorldNeedsUpdate],[yt.prototype._setValue_fromArray,yt.prototype._setValue_fromArray_setNeedsUpdate,yt.prototype._setValue_fromArray_setMatrixWorldNeedsUpdate]];var U3=new Float32Array(1);var Bh=class n{static{n.prototype.isMatrix2=!0}constructor(e,t,i,r){this.elements=[1,0,0,1],e!==void 0&&this.set(e,t,i,r)}identity(){return this.set(1,0,0,1),this}fromArray(e,t=0){for(let i=0;i<4;i++)this.elements[i]=e[i+t];return this}set(e,t,i,r){let s=this.elements;return s[0]=e,s[2]=t,s[1]=i,s[3]=r,this}};function dp(n,e,t,i){let r=Nb(i);switch(t){case np:return n*e;case rp:return n*e/r.components*r.byteLength;case Dl:return n*e/r.components*r.byteLength;case Xi:return n*e*2/r.components*r.byteLength;case Rl:return n*e*2/r.components*r.byteLength;case ip:return n*e*3/r.components*r.byteLength;case bn:return n*e*4/r.components*r.byteLength;case Nl:return n*e*4/r.components*r.byteLength;case Jo:case Ko:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Qo:case ea:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Ll:case Fl:return Math.max(n,16)*Math.max(e,8)/4;case Pl:case Ol:return Math.max(n,8)*Math.max(e,8)/2;case kl:case Ul:case Vl:case Hl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*8;case Bl:case ta:case zl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Gl:return Math.floor((n+3)/4)*Math.floor((e+3)/4)*16;case Wl:return Math.floor((n+4)/5)*Math.floor((e+3)/4)*16;case jl:return Math.floor((n+4)/5)*Math.floor((e+4)/5)*16;case $l:return Math.floor((n+5)/6)*Math.floor((e+4)/5)*16;case ql:return Math.floor((n+5)/6)*Math.floor((e+5)/6)*16;case Xl:return Math.floor((n+7)/8)*Math.floor((e+4)/5)*16;case Yl:return Math.floor((n+7)/8)*Math.floor((e+5)/6)*16;case Zl:return Math.floor((n+7)/8)*Math.floor((e+7)/8)*16;case Jl:return Math.floor((n+9)/10)*Math.floor((e+4)/5)*16;case Kl:return Math.floor((n+9)/10)*Math.floor((e+5)/6)*16;case Ql:return Math.floor((n+9)/10)*Math.floor((e+7)/8)*16;case eu:return Math.floor((n+9)/10)*Math.floor((e+9)/10)*16;case tu:return Math.floor((n+11)/12)*Math.floor((e+9)/10)*16;case nu:return Math.floor((n+11)/12)*Math.floor((e+11)/12)*16;case iu:case ru:case su:return Math.ceil(n/4)*Math.ceil(e/4)*16;case ou:case au:return Math.ceil(n/4)*Math.ceil(e/4)*8;case na:case cu:return Math.ceil(n/4)*Math.ceil(e/4)*16}throw new Error(`Unable to determine texture byte length for ${t} format.`)}function Nb(n){switch(n){case pn:case Kh:return{byteLength:1,components:1};case Ls:case Qh:case ei:return{byteLength:2,components:1};case Cl:case Al:return{byteLength:2,components:4};case kn:case Il:case Un:return{byteLength:4,components:1};case ep:case tp:return{byteLength:4,components:3}}throw new Error(`Unknown texture type ${n}.`)}typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("register",{detail:{revision:"184"}}));typeof window<"u"&&(window.__THREE__?be("WARNING: Multiple instances of Three.js being imported."):window.__THREE__="184");function k_(){let n=null,e=!1,t=null,i=null;function r(s,o){t(s,o),i=n.requestAnimationFrame(r)}return{start:function(){e!==!0&&t!==null&&n!==null&&(i=n.requestAnimationFrame(r),e=!0)},stop:function(){n!==null&&n.cancelAnimationFrame(i),e=!1},setAnimationLoop:function(s){t=s},setContext:function(s){n=s}}}function Lb(n){let e=new WeakMap;function t(a,c){let l=a.array,u=a.usage,f=l.byteLength,d=n.createBuffer();n.bindBuffer(c,d),n.bufferData(c,l,u),a.onUploadCallback();let h;if(l instanceof Float32Array)h=n.FLOAT;else if(typeof Float16Array<"u"&&l instanceof Float16Array)h=n.HALF_FLOAT;else if(l instanceof Uint16Array)a.isFloat16BufferAttribute?h=n.HALF_FLOAT:h=n.UNSIGNED_SHORT;else if(l instanceof Int16Array)h=n.SHORT;else if(l instanceof Uint32Array)h=n.UNSIGNED_INT;else if(l instanceof Int32Array)h=n.INT;else if(l instanceof Int8Array)h=n.BYTE;else if(l instanceof Uint8Array)h=n.UNSIGNED_BYTE;else if(l instanceof Uint8ClampedArray)h=n.UNSIGNED_BYTE;else throw new Error("THREE.WebGLAttributes: Unsupported buffer data format: "+l);return{buffer:d,type:h,bytesPerElement:l.BYTES_PER_ELEMENT,version:a.version,size:f}}function i(a,c,l){let u=c.array,f=c.updateRanges;if(n.bindBuffer(l,a),f.length===0)n.bufferSubData(l,0,u);else{f.sort((h,v)=>h.start-v.start);let d=0;for(let h=1;h<f.length;h++){let v=f[d],M=f[h];M.start<=v.start+v.count+1?v.count=Math.max(v.count,M.start+M.count-v.start):(++d,f[d]=M)}f.length=d+1;for(let h=0,v=f.length;h<v;h++){let M=f[h];n.bufferSubData(l,M.start*u.BYTES_PER_ELEMENT,u,M.start,M.count)}c.clearUpdateRanges()}c.onUploadCallback()}function r(a){return a.isInterleavedBufferAttribute&&(a=a.data),e.get(a)}function s(a){a.isInterleavedBufferAttribute&&(a=a.data);let c=e.get(a);c&&(n.deleteBuffer(c.buffer),e.delete(a))}function o(a,c){if(a.isInterleavedBufferAttribute&&(a=a.data),a.isGLBufferAttribute){let u=e.get(a);(!u||u.version<a.version)&&e.set(a,{buffer:a.buffer,type:a.type,bytesPerElement:a.elementSize,version:a.version});return}let l=e.get(a);if(l===void 0)e.set(a,t(a,c));else if(l.version<a.version){if(l.size!==a.array.byteLength)throw new Error("THREE.WebGLAttributes: The size of the buffer attribute's array buffer does not match the original size. Resizing buffer attributes is not supported.");i(l.buffer,a,c),l.version=a.version}}return{get:r,remove:s,update:o}}var Ob=`#ifdef USE_ALPHAHASH
	if ( diffuseColor.a < getAlphaHashThreshold( vPosition ) ) discard;
#endif`,Fb=`#ifdef USE_ALPHAHASH
	const float ALPHA_HASH_SCALE = 0.05;
	float hash2D( vec2 value ) {
		return fract( 1.0e4 * sin( 17.0 * value.x + 0.1 * value.y ) * ( 0.1 + abs( sin( 13.0 * value.y + value.x ) ) ) );
	}
	float hash3D( vec3 value ) {
		return hash2D( vec2( hash2D( value.xy ), value.z ) );
	}
	float getAlphaHashThreshold( vec3 position ) {
		float maxDeriv = max(
			length( dFdx( position.xyz ) ),
			length( dFdy( position.xyz ) )
		);
		float pixScale = 1.0 / ( ALPHA_HASH_SCALE * maxDeriv );
		vec2 pixScales = vec2(
			exp2( floor( log2( pixScale ) ) ),
			exp2( ceil( log2( pixScale ) ) )
		);
		vec2 alpha = vec2(
			hash3D( floor( pixScales.x * position.xyz ) ),
			hash3D( floor( pixScales.y * position.xyz ) )
		);
		float lerpFactor = fract( log2( pixScale ) );
		float x = ( 1.0 - lerpFactor ) * alpha.x + lerpFactor * alpha.y;
		float a = min( lerpFactor, 1.0 - lerpFactor );
		vec3 cases = vec3(
			x * x / ( 2.0 * a * ( 1.0 - a ) ),
			( x - 0.5 * a ) / ( 1.0 - a ),
			1.0 - ( ( 1.0 - x ) * ( 1.0 - x ) / ( 2.0 * a * ( 1.0 - a ) ) )
		);
		float threshold = ( x < ( 1.0 - a ) )
			? ( ( x < a ) ? cases.x : cases.y )
			: cases.z;
		return clamp( threshold , 1.0e-6, 1.0 );
	}
#endif`,kb=`#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, vAlphaMapUv ).g;
#endif`,Ub=`#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,Bb=`#ifdef USE_ALPHATEST
	#ifdef ALPHA_TO_COVERAGE
	diffuseColor.a = smoothstep( alphaTest, alphaTest + fwidth( diffuseColor.a ), diffuseColor.a );
	if ( diffuseColor.a == 0.0 ) discard;
	#else
	if ( diffuseColor.a < alphaTest ) discard;
	#endif
#endif`,Vb=`#ifdef USE_ALPHATEST
	uniform float alphaTest;
#endif`,Hb=`#ifdef USE_AOMAP
	float ambientOcclusion = ( texture2D( aoMap, vAoMapUv ).r - 1.0 ) * aoMapIntensity + 1.0;
	reflectedLight.indirectDiffuse *= ambientOcclusion;
	#if defined( USE_CLEARCOAT ) 
		clearcoatSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_SHEEN ) 
		sheenSpecularIndirect *= ambientOcclusion;
	#endif
	#if defined( USE_ENVMAP ) && defined( STANDARD )
		float dotNV = saturate( dot( geometryNormal, geometryViewDir ) );
		reflectedLight.indirectSpecular *= computeSpecularOcclusion( dotNV, ambientOcclusion, material.roughness );
	#endif
#endif`,zb=`#ifdef USE_AOMAP
	uniform sampler2D aoMap;
	uniform float aoMapIntensity;
#endif`,Gb=`#ifdef USE_BATCHING
	#if ! defined( GL_ANGLE_multi_draw )
	#define gl_DrawID _gl_DrawID
	uniform int _gl_DrawID;
	#endif
	uniform highp sampler2D batchingTexture;
	uniform highp usampler2D batchingIdTexture;
	mat4 getBatchingMatrix( const in float i ) {
		int size = textureSize( batchingTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( batchingTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( batchingTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( batchingTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( batchingTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
	float getIndirectIndex( const in int i ) {
		int size = textureSize( batchingIdTexture, 0 ).x;
		int x = i % size;
		int y = i / size;
		return float( texelFetch( batchingIdTexture, ivec2( x, y ), 0 ).r );
	}
#endif
#ifdef USE_BATCHING_COLOR
	uniform sampler2D batchingColorTexture;
	vec4 getBatchingColor( const in float i ) {
		int size = textureSize( batchingColorTexture, 0 ).x;
		int j = int( i );
		int x = j % size;
		int y = j / size;
		return texelFetch( batchingColorTexture, ivec2( x, y ), 0 );
	}
#endif`,Wb=`#ifdef USE_BATCHING
	mat4 batchingMatrix = getBatchingMatrix( getIndirectIndex( gl_DrawID ) );
#endif`,jb=`vec3 transformed = vec3( position );
#ifdef USE_ALPHAHASH
	vPosition = vec3( position );
#endif`,$b=`vec3 objectNormal = vec3( normal );
#ifdef USE_TANGENT
	vec3 objectTangent = vec3( tangent.xyz );
#endif`,qb=`float G_BlinnPhong_Implicit( ) {
	return 0.25;
}
float D_BlinnPhong( const in float shininess, const in float dotNH ) {
	return RECIPROCAL_PI * ( shininess * 0.5 + 1.0 ) * pow( dotNH, shininess );
}
vec3 BRDF_BlinnPhong( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in vec3 specularColor, const in float shininess ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( specularColor, 1.0, dotVH );
	float G = G_BlinnPhong_Implicit( );
	float D = D_BlinnPhong( shininess, dotNH );
	return F * ( G * D );
} // validated`,Xb=`#ifdef USE_IRIDESCENCE
	const mat3 XYZ_TO_REC709 = mat3(
		 3.2404542, -0.9692660,  0.0556434,
		-1.5371385,  1.8760108, -0.2040259,
		-0.4985314,  0.0415560,  1.0572252
	);
	vec3 Fresnel0ToIor( vec3 fresnel0 ) {
		vec3 sqrtF0 = sqrt( fresnel0 );
		return ( vec3( 1.0 ) + sqrtF0 ) / ( vec3( 1.0 ) - sqrtF0 );
	}
	vec3 IorToFresnel0( vec3 transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - vec3( incidentIor ) ) / ( transmittedIor + vec3( incidentIor ) ) );
	}
	float IorToFresnel0( float transmittedIor, float incidentIor ) {
		return pow2( ( transmittedIor - incidentIor ) / ( transmittedIor + incidentIor ));
	}
	vec3 evalSensitivity( float OPD, vec3 shift ) {
		float phase = 2.0 * PI * OPD * 1.0e-9;
		vec3 val = vec3( 5.4856e-13, 4.4201e-13, 5.2481e-13 );
		vec3 pos = vec3( 1.6810e+06, 1.7953e+06, 2.2084e+06 );
		vec3 var = vec3( 4.3278e+09, 9.3046e+09, 6.6121e+09 );
		vec3 xyz = val * sqrt( 2.0 * PI * var ) * cos( pos * phase + shift ) * exp( - pow2( phase ) * var );
		xyz.x += 9.7470e-14 * sqrt( 2.0 * PI * 4.5282e+09 ) * cos( 2.2399e+06 * phase + shift[ 0 ] ) * exp( - 4.5282e+09 * pow2( phase ) );
		xyz /= 1.0685e-7;
		vec3 rgb = XYZ_TO_REC709 * xyz;
		return rgb;
	}
	vec3 evalIridescence( float outsideIOR, float eta2, float cosTheta1, float thinFilmThickness, vec3 baseF0 ) {
		vec3 I;
		float iridescenceIOR = mix( outsideIOR, eta2, smoothstep( 0.0, 0.03, thinFilmThickness ) );
		float sinTheta2Sq = pow2( outsideIOR / iridescenceIOR ) * ( 1.0 - pow2( cosTheta1 ) );
		float cosTheta2Sq = 1.0 - sinTheta2Sq;
		if ( cosTheta2Sq < 0.0 ) {
			return vec3( 1.0 );
		}
		float cosTheta2 = sqrt( cosTheta2Sq );
		float R0 = IorToFresnel0( iridescenceIOR, outsideIOR );
		float R12 = F_Schlick( R0, 1.0, cosTheta1 );
		float T121 = 1.0 - R12;
		float phi12 = 0.0;
		if ( iridescenceIOR < outsideIOR ) phi12 = PI;
		float phi21 = PI - phi12;
		vec3 baseIOR = Fresnel0ToIor( clamp( baseF0, 0.0, 0.9999 ) );		vec3 R1 = IorToFresnel0( baseIOR, iridescenceIOR );
		vec3 R23 = F_Schlick( R1, 1.0, cosTheta2 );
		vec3 phi23 = vec3( 0.0 );
		if ( baseIOR[ 0 ] < iridescenceIOR ) phi23[ 0 ] = PI;
		if ( baseIOR[ 1 ] < iridescenceIOR ) phi23[ 1 ] = PI;
		if ( baseIOR[ 2 ] < iridescenceIOR ) phi23[ 2 ] = PI;
		float OPD = 2.0 * iridescenceIOR * thinFilmThickness * cosTheta2;
		vec3 phi = vec3( phi21 ) + phi23;
		vec3 R123 = clamp( R12 * R23, 1e-5, 0.9999 );
		vec3 r123 = sqrt( R123 );
		vec3 Rs = pow2( T121 ) * R23 / ( vec3( 1.0 ) - R123 );
		vec3 C0 = R12 + Rs;
		I = C0;
		vec3 Cm = Rs - T121;
		for ( int m = 1; m <= 2; ++ m ) {
			Cm *= r123;
			vec3 Sm = 2.0 * evalSensitivity( float( m ) * OPD, float( m ) * phi );
			I += Cm * Sm;
		}
		return max( I, vec3( 0.0 ) );
	}
#endif`,Yb=`#ifdef USE_BUMPMAP
	uniform sampler2D bumpMap;
	uniform float bumpScale;
	vec2 dHdxy_fwd() {
		vec2 dSTdx = dFdx( vBumpMapUv );
		vec2 dSTdy = dFdy( vBumpMapUv );
		float Hll = bumpScale * texture2D( bumpMap, vBumpMapUv ).x;
		float dBx = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdx ).x - Hll;
		float dBy = bumpScale * texture2D( bumpMap, vBumpMapUv + dSTdy ).x - Hll;
		return vec2( dBx, dBy );
	}
	vec3 perturbNormalArb( vec3 surf_pos, vec3 surf_norm, vec2 dHdxy, float faceDirection ) {
		vec3 vSigmaX = normalize( dFdx( surf_pos.xyz ) );
		vec3 vSigmaY = normalize( dFdy( surf_pos.xyz ) );
		vec3 vN = surf_norm;
		vec3 R1 = cross( vSigmaY, vN );
		vec3 R2 = cross( vN, vSigmaX );
		float fDet = dot( vSigmaX, R1 ) * faceDirection;
		vec3 vGrad = sign( fDet ) * ( dHdxy.x * R1 + dHdxy.y * R2 );
		return normalize( abs( fDet ) * surf_norm - vGrad );
	}
#endif`,Zb=`#if NUM_CLIPPING_PLANES > 0
	vec4 plane;
	#ifdef ALPHA_TO_COVERAGE
		float distanceToPlane, distanceGradient;
		float clipOpacity = 1.0;
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
			distanceGradient = fwidth( distanceToPlane ) / 2.0;
			clipOpacity *= smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			if ( clipOpacity == 0.0 ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			float unionClipOpacity = 1.0;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				distanceToPlane = - dot( vClipPosition, plane.xyz ) + plane.w;
				distanceGradient = fwidth( distanceToPlane ) / 2.0;
				unionClipOpacity *= 1.0 - smoothstep( - distanceGradient, distanceGradient, distanceToPlane );
			}
			#pragma unroll_loop_end
			clipOpacity *= 1.0 - unionClipOpacity;
		#endif
		diffuseColor.a *= clipOpacity;
		if ( diffuseColor.a == 0.0 ) discard;
	#else
		#pragma unroll_loop_start
		for ( int i = 0; i < UNION_CLIPPING_PLANES; i ++ ) {
			plane = clippingPlanes[ i ];
			if ( dot( vClipPosition, plane.xyz ) > plane.w ) discard;
		}
		#pragma unroll_loop_end
		#if UNION_CLIPPING_PLANES < NUM_CLIPPING_PLANES
			bool clipped = true;
			#pragma unroll_loop_start
			for ( int i = UNION_CLIPPING_PLANES; i < NUM_CLIPPING_PLANES; i ++ ) {
				plane = clippingPlanes[ i ];
				clipped = ( dot( vClipPosition, plane.xyz ) > plane.w ) && clipped;
			}
			#pragma unroll_loop_end
			if ( clipped ) discard;
		#endif
	#endif
#endif`,Jb=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
	uniform vec4 clippingPlanes[ NUM_CLIPPING_PLANES ];
#endif`,Kb=`#if NUM_CLIPPING_PLANES > 0
	varying vec3 vClipPosition;
#endif`,Qb=`#if NUM_CLIPPING_PLANES > 0
	vClipPosition = - mvPosition.xyz;
#endif`,eT=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	diffuseColor *= vColor;
#endif`,tT=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA )
	varying vec4 vColor;
#endif`,nT=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	varying vec4 vColor;
#endif`,iT=`#if defined( USE_COLOR ) || defined( USE_COLOR_ALPHA ) || defined( USE_INSTANCING_COLOR ) || defined( USE_BATCHING_COLOR )
	vColor = vec4( 1.0 );
#endif
#ifdef USE_COLOR_ALPHA
	vColor *= color;
#elif defined( USE_COLOR )
	vColor.rgb *= color;
#endif
#ifdef USE_INSTANCING_COLOR
	vColor.rgb *= instanceColor.rgb;
#endif
#ifdef USE_BATCHING_COLOR
	vColor *= getBatchingColor( getIndirectIndex( gl_DrawID ) );
#endif`,rT=`#define PI 3.141592653589793
#define PI2 6.283185307179586
#define PI_HALF 1.5707963267948966
#define RECIPROCAL_PI 0.3183098861837907
#define RECIPROCAL_PI2 0.15915494309189535
#define EPSILON 1e-6
#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
#define whiteComplement( a ) ( 1.0 - saturate( a ) )
float pow2( const in float x ) { return x*x; }
vec3 pow2( const in vec3 x ) { return x*x; }
float pow3( const in float x ) { return x*x*x; }
float pow4( const in float x ) { float x2 = x*x; return x2*x2; }
float max3( const in vec3 v ) { return max( max( v.x, v.y ), v.z ); }
float average( const in vec3 v ) { return dot( v, vec3( 0.3333333 ) ); }
highp float rand( const in vec2 uv ) {
	const highp float a = 12.9898, b = 78.233, c = 43758.5453;
	highp float dt = dot( uv.xy, vec2( a,b ) ), sn = mod( dt, PI );
	return fract( sin( sn ) * c );
}
#ifdef HIGH_PRECISION
	float precisionSafeLength( vec3 v ) { return length( v ); }
#else
	float precisionSafeLength( vec3 v ) {
		float maxComponent = max3( abs( v ) );
		return length( v / maxComponent ) * maxComponent;
	}
#endif
struct IncidentLight {
	vec3 color;
	vec3 direction;
	bool visible;
};
struct ReflectedLight {
	vec3 directDiffuse;
	vec3 directSpecular;
	vec3 indirectDiffuse;
	vec3 indirectSpecular;
};
#ifdef USE_ALPHAHASH
	varying vec3 vPosition;
#endif
vec3 transformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );
}
vec3 inverseTransformDirection( in vec3 dir, in mat4 matrix ) {
	return normalize( ( vec4( dir, 0.0 ) * matrix ).xyz );
}
bool isPerspectiveMatrix( mat4 m ) {
	return m[ 2 ][ 3 ] == - 1.0;
}
vec2 equirectUv( in vec3 dir ) {
	float u = atan( dir.z, dir.x ) * RECIPROCAL_PI2 + 0.5;
	float v = asin( clamp( dir.y, - 1.0, 1.0 ) ) * RECIPROCAL_PI + 0.5;
	return vec2( u, v );
}
vec3 BRDF_Lambert( const in vec3 diffuseColor ) {
	return RECIPROCAL_PI * diffuseColor;
}
vec3 F_Schlick( const in vec3 f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
}
float F_Schlick( const in float f0, const in float f90, const in float dotVH ) {
	float fresnel = exp2( ( - 5.55473 * dotVH - 6.98316 ) * dotVH );
	return f0 * ( 1.0 - fresnel ) + ( f90 * fresnel );
} // validated`,sT=`#ifdef ENVMAP_TYPE_CUBE_UV
	#define cubeUV_minMipLevel 4.0
	#define cubeUV_minTileSize 16.0
	float getFace( vec3 direction ) {
		vec3 absDirection = abs( direction );
		float face = - 1.0;
		if ( absDirection.x > absDirection.z ) {
			if ( absDirection.x > absDirection.y )
				face = direction.x > 0.0 ? 0.0 : 3.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		} else {
			if ( absDirection.z > absDirection.y )
				face = direction.z > 0.0 ? 2.0 : 5.0;
			else
				face = direction.y > 0.0 ? 1.0 : 4.0;
		}
		return face;
	}
	vec2 getUV( vec3 direction, float face ) {
		vec2 uv;
		if ( face == 0.0 ) {
			uv = vec2( direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 1.0 ) {
			uv = vec2( - direction.x, - direction.z ) / abs( direction.y );
		} else if ( face == 2.0 ) {
			uv = vec2( - direction.x, direction.y ) / abs( direction.z );
		} else if ( face == 3.0 ) {
			uv = vec2( - direction.z, direction.y ) / abs( direction.x );
		} else if ( face == 4.0 ) {
			uv = vec2( - direction.x, direction.z ) / abs( direction.y );
		} else {
			uv = vec2( direction.x, direction.y ) / abs( direction.z );
		}
		return 0.5 * ( uv + 1.0 );
	}
	vec3 bilinearCubeUV( sampler2D envMap, vec3 direction, float mipInt ) {
		float face = getFace( direction );
		float filterInt = max( cubeUV_minMipLevel - mipInt, 0.0 );
		mipInt = max( mipInt, cubeUV_minMipLevel );
		float faceSize = exp2( mipInt );
		highp vec2 uv = getUV( direction, face ) * ( faceSize - 2.0 ) + 1.0;
		if ( face > 2.0 ) {
			uv.y += faceSize;
			face -= 3.0;
		}
		uv.x += face * faceSize;
		uv.x += filterInt * 3.0 * cubeUV_minTileSize;
		uv.y += 4.0 * ( exp2( CUBEUV_MAX_MIP ) - faceSize );
		uv.x *= CUBEUV_TEXEL_WIDTH;
		uv.y *= CUBEUV_TEXEL_HEIGHT;
		#ifdef texture2DGradEXT
			return texture2DGradEXT( envMap, uv, vec2( 0.0 ), vec2( 0.0 ) ).rgb;
		#else
			return texture2D( envMap, uv ).rgb;
		#endif
	}
	#define cubeUV_r0 1.0
	#define cubeUV_m0 - 2.0
	#define cubeUV_r1 0.8
	#define cubeUV_m1 - 1.0
	#define cubeUV_r4 0.4
	#define cubeUV_m4 2.0
	#define cubeUV_r5 0.305
	#define cubeUV_m5 3.0
	#define cubeUV_r6 0.21
	#define cubeUV_m6 4.0
	float roughnessToMip( float roughness ) {
		float mip = 0.0;
		if ( roughness >= cubeUV_r1 ) {
			mip = ( cubeUV_r0 - roughness ) * ( cubeUV_m1 - cubeUV_m0 ) / ( cubeUV_r0 - cubeUV_r1 ) + cubeUV_m0;
		} else if ( roughness >= cubeUV_r4 ) {
			mip = ( cubeUV_r1 - roughness ) * ( cubeUV_m4 - cubeUV_m1 ) / ( cubeUV_r1 - cubeUV_r4 ) + cubeUV_m1;
		} else if ( roughness >= cubeUV_r5 ) {
			mip = ( cubeUV_r4 - roughness ) * ( cubeUV_m5 - cubeUV_m4 ) / ( cubeUV_r4 - cubeUV_r5 ) + cubeUV_m4;
		} else if ( roughness >= cubeUV_r6 ) {
			mip = ( cubeUV_r5 - roughness ) * ( cubeUV_m6 - cubeUV_m5 ) / ( cubeUV_r5 - cubeUV_r6 ) + cubeUV_m5;
		} else {
			mip = - 2.0 * log2( 1.16 * roughness );		}
		return mip;
	}
	vec4 textureCubeUV( sampler2D envMap, vec3 sampleDir, float roughness ) {
		float mip = clamp( roughnessToMip( roughness ), cubeUV_m0, CUBEUV_MAX_MIP );
		float mipF = fract( mip );
		float mipInt = floor( mip );
		vec3 color0 = bilinearCubeUV( envMap, sampleDir, mipInt );
		if ( mipF == 0.0 ) {
			return vec4( color0, 1.0 );
		} else {
			vec3 color1 = bilinearCubeUV( envMap, sampleDir, mipInt + 1.0 );
			return vec4( mix( color0, color1, mipF ), 1.0 );
		}
	}
#endif`,oT=`vec3 transformedNormal = objectNormal;
#ifdef USE_TANGENT
	vec3 transformedTangent = objectTangent;
#endif
#ifdef USE_BATCHING
	mat3 bm = mat3( batchingMatrix );
	transformedNormal /= vec3( dot( bm[ 0 ], bm[ 0 ] ), dot( bm[ 1 ], bm[ 1 ] ), dot( bm[ 2 ], bm[ 2 ] ) );
	transformedNormal = bm * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = bm * transformedTangent;
	#endif
#endif
#ifdef USE_INSTANCING
	mat3 im = mat3( instanceMatrix );
	transformedNormal /= vec3( dot( im[ 0 ], im[ 0 ] ), dot( im[ 1 ], im[ 1 ] ), dot( im[ 2 ], im[ 2 ] ) );
	transformedNormal = im * transformedNormal;
	#ifdef USE_TANGENT
		transformedTangent = im * transformedTangent;
	#endif
#endif
transformedNormal = normalMatrix * transformedNormal;
#ifdef FLIP_SIDED
	transformedNormal = - transformedNormal;
#endif
#ifdef USE_TANGENT
	transformedTangent = ( modelViewMatrix * vec4( transformedTangent, 0.0 ) ).xyz;
	#ifdef FLIP_SIDED
		transformedTangent = - transformedTangent;
	#endif
#endif`,aT=`#ifdef USE_DISPLACEMENTMAP
	uniform sampler2D displacementMap;
	uniform float displacementScale;
	uniform float displacementBias;
#endif`,cT=`#ifdef USE_DISPLACEMENTMAP
	transformed += normalize( objectNormal ) * ( texture2D( displacementMap, vDisplacementMapUv ).x * displacementScale + displacementBias );
#endif`,lT=`#ifdef USE_EMISSIVEMAP
	vec4 emissiveColor = texture2D( emissiveMap, vEmissiveMapUv );
	#ifdef DECODE_VIDEO_TEXTURE_EMISSIVE
		emissiveColor = sRGBTransferEOTF( emissiveColor );
	#endif
	totalEmissiveRadiance *= emissiveColor.rgb;
#endif`,uT=`#ifdef USE_EMISSIVEMAP
	uniform sampler2D emissiveMap;
#endif`,dT="gl_FragColor = linearToOutputTexel( gl_FragColor );",fT=`vec4 LinearTransferOETF( in vec4 value ) {
	return value;
}
vec4 sRGBTransferEOTF( in vec4 value ) {
	return vec4( mix( pow( value.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), value.rgb * 0.0773993808, vec3( lessThanEqual( value.rgb, vec3( 0.04045 ) ) ) ), value.a );
}
vec4 sRGBTransferOETF( in vec4 value ) {
	return vec4( mix( pow( value.rgb, vec3( 0.41666 ) ) * 1.055 - vec3( 0.055 ), value.rgb * 12.92, vec3( lessThanEqual( value.rgb, vec3( 0.0031308 ) ) ) ), value.a );
}`,hT=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vec3 cameraToFrag;
		if ( isOrthographic ) {
			cameraToFrag = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToFrag = normalize( vWorldPosition - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vec3 reflectVec = reflect( cameraToFrag, worldNormal );
		#else
			vec3 reflectVec = refract( cameraToFrag, worldNormal, refractionRatio );
		#endif
	#else
		vec3 reflectVec = vReflect;
	#endif
	#ifdef ENVMAP_TYPE_CUBE
		vec4 envColor = textureCube( envMap, envMapRotation * reflectVec );
		#ifdef ENVMAP_BLENDING_MULTIPLY
			outgoingLight = mix( outgoingLight, outgoingLight * envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_MIX )
			outgoingLight = mix( outgoingLight, envColor.xyz, specularStrength * reflectivity );
		#elif defined( ENVMAP_BLENDING_ADD )
			outgoingLight += envColor.xyz * specularStrength * reflectivity;
		#endif
	#endif
#endif`,pT=`#ifdef USE_ENVMAP
	uniform float envMapIntensity;
	uniform mat3 envMapRotation;
	#ifdef ENVMAP_TYPE_CUBE
		uniform samplerCube envMap;
	#else
		uniform sampler2D envMap;
	#endif
#endif`,mT=`#ifdef USE_ENVMAP
	uniform float reflectivity;
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		varying vec3 vWorldPosition;
		uniform float refractionRatio;
	#else
		varying vec3 vReflect;
	#endif
#endif`,gT=`#ifdef USE_ENVMAP
	#if defined( USE_BUMPMAP ) || defined( USE_NORMALMAP ) || defined( PHONG ) || defined( LAMBERT )
		#define ENV_WORLDPOS
	#endif
	#ifdef ENV_WORLDPOS
		
		varying vec3 vWorldPosition;
	#else
		varying vec3 vReflect;
		uniform float refractionRatio;
	#endif
#endif`,vT=`#ifdef USE_ENVMAP
	#ifdef ENV_WORLDPOS
		vWorldPosition = worldPosition.xyz;
	#else
		vec3 cameraToVertex;
		if ( isOrthographic ) {
			cameraToVertex = normalize( vec3( - viewMatrix[ 0 ][ 2 ], - viewMatrix[ 1 ][ 2 ], - viewMatrix[ 2 ][ 2 ] ) );
		} else {
			cameraToVertex = normalize( worldPosition.xyz - cameraPosition );
		}
		vec3 worldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
		#ifdef ENVMAP_MODE_REFLECTION
			vReflect = reflect( cameraToVertex, worldNormal );
		#else
			vReflect = refract( cameraToVertex, worldNormal, refractionRatio );
		#endif
	#endif
#endif`,yT=`#ifdef USE_FOG
	vFogDepth = - mvPosition.z;
#endif`,_T=`#ifdef USE_FOG
	varying float vFogDepth;
#endif`,xT=`#ifdef USE_FOG
	#ifdef FOG_EXP2
		float fogFactor = 1.0 - exp( - fogDensity * fogDensity * vFogDepth * vFogDepth );
	#else
		float fogFactor = smoothstep( fogNear, fogFar, vFogDepth );
	#endif
	gl_FragColor.rgb = mix( gl_FragColor.rgb, fogColor, fogFactor );
#endif`,MT=`#ifdef USE_FOG
	uniform vec3 fogColor;
	varying float vFogDepth;
	#ifdef FOG_EXP2
		uniform float fogDensity;
	#else
		uniform float fogNear;
		uniform float fogFar;
	#endif
#endif`,ST=`#ifdef USE_GRADIENTMAP
	uniform sampler2D gradientMap;
#endif
vec3 getGradientIrradiance( vec3 normal, vec3 lightDirection ) {
	float dotNL = dot( normal, lightDirection );
	vec2 coord = vec2( dotNL * 0.5 + 0.5, 0.0 );
	#ifdef USE_GRADIENTMAP
		return vec3( texture2D( gradientMap, coord ).r );
	#else
		vec2 fw = fwidth( coord ) * 0.5;
		return mix( vec3( 0.7 ), vec3( 1.0 ), smoothstep( 0.7 - fw.x, 0.7 + fw.x, coord.x ) );
	#endif
}`,ET=`#ifdef USE_LIGHTMAP
	uniform sampler2D lightMap;
	uniform float lightMapIntensity;
#endif`,bT=`LambertMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularStrength = specularStrength;`,TT=`varying vec3 vViewPosition;
struct LambertMaterial {
	vec3 diffuseColor;
	float specularStrength;
};
void RE_Direct_Lambert( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Lambert( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in LambertMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Lambert
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Lambert`,wT=`uniform bool receiveShadow;
uniform vec3 ambientLightColor;
#if defined( USE_LIGHT_PROBES )
	uniform vec3 lightProbe[ 9 ];
#endif
vec3 shGetIrradianceAt( in vec3 normal, in vec3 shCoefficients[ 9 ] ) {
	float x = normal.x, y = normal.y, z = normal.z;
	vec3 result = shCoefficients[ 0 ] * 0.886227;
	result += shCoefficients[ 1 ] * 2.0 * 0.511664 * y;
	result += shCoefficients[ 2 ] * 2.0 * 0.511664 * z;
	result += shCoefficients[ 3 ] * 2.0 * 0.511664 * x;
	result += shCoefficients[ 4 ] * 2.0 * 0.429043 * x * y;
	result += shCoefficients[ 5 ] * 2.0 * 0.429043 * y * z;
	result += shCoefficients[ 6 ] * ( 0.743125 * z * z - 0.247708 );
	result += shCoefficients[ 7 ] * 2.0 * 0.429043 * x * z;
	result += shCoefficients[ 8 ] * 0.429043 * ( x * x - y * y );
	return result;
}
vec3 getLightProbeIrradiance( const in vec3 lightProbe[ 9 ], const in vec3 normal ) {
	vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
	vec3 irradiance = shGetIrradianceAt( worldNormal, lightProbe );
	return irradiance;
}
vec3 getAmbientLightIrradiance( const in vec3 ambientLightColor ) {
	vec3 irradiance = ambientLightColor;
	return irradiance;
}
float getDistanceAttenuation( const in float lightDistance, const in float cutoffDistance, const in float decayExponent ) {
	float distanceFalloff = 1.0 / max( pow( lightDistance, decayExponent ), 0.01 );
	if ( cutoffDistance > 0.0 ) {
		distanceFalloff *= pow2( saturate( 1.0 - pow4( lightDistance / cutoffDistance ) ) );
	}
	return distanceFalloff;
}
float getSpotAttenuation( const in float coneCosine, const in float penumbraCosine, const in float angleCosine ) {
	return smoothstep( coneCosine, penumbraCosine, angleCosine );
}
#if NUM_DIR_LIGHTS > 0
	struct DirectionalLight {
		vec3 direction;
		vec3 color;
	};
	uniform DirectionalLight directionalLights[ NUM_DIR_LIGHTS ];
	void getDirectionalLightInfo( const in DirectionalLight directionalLight, out IncidentLight light ) {
		light.color = directionalLight.color;
		light.direction = directionalLight.direction;
		light.visible = true;
	}
#endif
#if NUM_POINT_LIGHTS > 0
	struct PointLight {
		vec3 position;
		vec3 color;
		float distance;
		float decay;
	};
	uniform PointLight pointLights[ NUM_POINT_LIGHTS ];
	void getPointLightInfo( const in PointLight pointLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = pointLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float lightDistance = length( lVector );
		light.color = pointLight.color;
		light.color *= getDistanceAttenuation( lightDistance, pointLight.distance, pointLight.decay );
		light.visible = ( light.color != vec3( 0.0 ) );
	}
#endif
#if NUM_SPOT_LIGHTS > 0
	struct SpotLight {
		vec3 position;
		vec3 direction;
		vec3 color;
		float distance;
		float decay;
		float coneCos;
		float penumbraCos;
	};
	uniform SpotLight spotLights[ NUM_SPOT_LIGHTS ];
	void getSpotLightInfo( const in SpotLight spotLight, const in vec3 geometryPosition, out IncidentLight light ) {
		vec3 lVector = spotLight.position - geometryPosition;
		light.direction = normalize( lVector );
		float angleCos = dot( light.direction, spotLight.direction );
		float spotAttenuation = getSpotAttenuation( spotLight.coneCos, spotLight.penumbraCos, angleCos );
		if ( spotAttenuation > 0.0 ) {
			float lightDistance = length( lVector );
			light.color = spotLight.color * spotAttenuation;
			light.color *= getDistanceAttenuation( lightDistance, spotLight.distance, spotLight.decay );
			light.visible = ( light.color != vec3( 0.0 ) );
		} else {
			light.color = vec3( 0.0 );
			light.visible = false;
		}
	}
#endif
#if NUM_RECT_AREA_LIGHTS > 0
	struct RectAreaLight {
		vec3 color;
		vec3 position;
		vec3 halfWidth;
		vec3 halfHeight;
	};
	uniform sampler2D ltc_1;	uniform sampler2D ltc_2;
	uniform RectAreaLight rectAreaLights[ NUM_RECT_AREA_LIGHTS ];
#endif
#if NUM_HEMI_LIGHTS > 0
	struct HemisphereLight {
		vec3 direction;
		vec3 skyColor;
		vec3 groundColor;
	};
	uniform HemisphereLight hemisphereLights[ NUM_HEMI_LIGHTS ];
	vec3 getHemisphereLightIrradiance( const in HemisphereLight hemiLight, const in vec3 normal ) {
		float dotNL = dot( normal, hemiLight.direction );
		float hemiDiffuseWeight = 0.5 * dotNL + 0.5;
		vec3 irradiance = mix( hemiLight.groundColor, hemiLight.skyColor, hemiDiffuseWeight );
		return irradiance;
	}
#endif
#include <lightprobes_pars_fragment>`,IT=`#ifdef USE_ENVMAP
	vec3 getIBLIrradiance( const in vec3 normal ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 worldNormal = inverseTransformDirection( normal, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * worldNormal, 1.0 );
			return PI * envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	vec3 getIBLRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness ) {
		#ifdef ENVMAP_TYPE_CUBE_UV
			vec3 reflectVec = reflect( - viewDir, normal );
			reflectVec = normalize( mix( reflectVec, normal, pow4( roughness ) ) );
			reflectVec = inverseTransformDirection( reflectVec, viewMatrix );
			vec4 envMapColor = textureCubeUV( envMap, envMapRotation * reflectVec, roughness );
			return envMapColor.rgb * envMapIntensity;
		#else
			return vec3( 0.0 );
		#endif
	}
	#ifdef USE_ANISOTROPY
		vec3 getIBLAnisotropyRadiance( const in vec3 viewDir, const in vec3 normal, const in float roughness, const in vec3 bitangent, const in float anisotropy ) {
			#ifdef ENVMAP_TYPE_CUBE_UV
				vec3 bentNormal = cross( bitangent, viewDir );
				bentNormal = normalize( cross( bentNormal, bitangent ) );
				bentNormal = normalize( mix( bentNormal, normal, pow2( pow2( 1.0 - anisotropy * ( 1.0 - roughness ) ) ) ) );
				return getIBLRadiance( viewDir, bentNormal, roughness );
			#else
				return vec3( 0.0 );
			#endif
		}
	#endif
#endif`,CT=`ToonMaterial material;
material.diffuseColor = diffuseColor.rgb;`,AT=`varying vec3 vViewPosition;
struct ToonMaterial {
	vec3 diffuseColor;
};
void RE_Direct_Toon( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 irradiance = getGradientIrradiance( geometryNormal, directLight.direction ) * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
void RE_IndirectDiffuse_Toon( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in ToonMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_Toon
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Toon`,DT=`BlinnPhongMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.specularColor = specular;
material.specularShininess = shininess;
material.specularStrength = specularStrength;`,RT=`varying vec3 vViewPosition;
struct BlinnPhongMaterial {
	vec3 diffuseColor;
	vec3 specularColor;
	float specularShininess;
	float specularStrength;
};
void RE_Direct_BlinnPhong( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
	reflectedLight.directSpecular += irradiance * BRDF_BlinnPhong( directLight.direction, geometryViewDir, geometryNormal, material.specularColor, material.specularShininess ) * material.specularStrength;
}
void RE_IndirectDiffuse_BlinnPhong( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in BlinnPhongMaterial material, inout ReflectedLight reflectedLight ) {
	reflectedLight.indirectDiffuse += irradiance * BRDF_Lambert( material.diffuseColor );
}
#define RE_Direct				RE_Direct_BlinnPhong
#define RE_IndirectDiffuse		RE_IndirectDiffuse_BlinnPhong`,NT=`PhysicalMaterial material;
material.diffuseColor = diffuseColor.rgb;
material.diffuseContribution = diffuseColor.rgb * ( 1.0 - metalnessFactor );
material.metalness = metalnessFactor;
vec3 dxy = max( abs( dFdx( nonPerturbedNormal ) ), abs( dFdy( nonPerturbedNormal ) ) );
float geometryRoughness = max( max( dxy.x, dxy.y ), dxy.z );
material.roughness = max( roughnessFactor, 0.0525 );material.roughness += geometryRoughness;
material.roughness = min( material.roughness, 1.0 );
#ifdef IOR
	material.ior = ior;
	#ifdef USE_SPECULAR
		float specularIntensityFactor = specularIntensity;
		vec3 specularColorFactor = specularColor;
		#ifdef USE_SPECULAR_COLORMAP
			specularColorFactor *= texture2D( specularColorMap, vSpecularColorMapUv ).rgb;
		#endif
		#ifdef USE_SPECULAR_INTENSITYMAP
			specularIntensityFactor *= texture2D( specularIntensityMap, vSpecularIntensityMapUv ).a;
		#endif
		material.specularF90 = mix( specularIntensityFactor, 1.0, metalnessFactor );
	#else
		float specularIntensityFactor = 1.0;
		vec3 specularColorFactor = vec3( 1.0 );
		material.specularF90 = 1.0;
	#endif
	material.specularColor = min( pow2( ( material.ior - 1.0 ) / ( material.ior + 1.0 ) ) * specularColorFactor, vec3( 1.0 ) ) * specularIntensityFactor;
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
#else
	material.specularColor = vec3( 0.04 );
	material.specularColorBlended = mix( material.specularColor, diffuseColor.rgb, metalnessFactor );
	material.specularF90 = 1.0;
#endif
#ifdef USE_CLEARCOAT
	material.clearcoat = clearcoat;
	material.clearcoatRoughness = clearcoatRoughness;
	material.clearcoatF0 = vec3( 0.04 );
	material.clearcoatF90 = 1.0;
	#ifdef USE_CLEARCOATMAP
		material.clearcoat *= texture2D( clearcoatMap, vClearcoatMapUv ).x;
	#endif
	#ifdef USE_CLEARCOAT_ROUGHNESSMAP
		material.clearcoatRoughness *= texture2D( clearcoatRoughnessMap, vClearcoatRoughnessMapUv ).y;
	#endif
	material.clearcoat = saturate( material.clearcoat );	material.clearcoatRoughness = max( material.clearcoatRoughness, 0.0525 );
	material.clearcoatRoughness += geometryRoughness;
	material.clearcoatRoughness = min( material.clearcoatRoughness, 1.0 );
#endif
#ifdef USE_DISPERSION
	material.dispersion = dispersion;
#endif
#ifdef USE_IRIDESCENCE
	material.iridescence = iridescence;
	material.iridescenceIOR = iridescenceIOR;
	#ifdef USE_IRIDESCENCEMAP
		material.iridescence *= texture2D( iridescenceMap, vIridescenceMapUv ).r;
	#endif
	#ifdef USE_IRIDESCENCE_THICKNESSMAP
		material.iridescenceThickness = (iridescenceThicknessMaximum - iridescenceThicknessMinimum) * texture2D( iridescenceThicknessMap, vIridescenceThicknessMapUv ).g + iridescenceThicknessMinimum;
	#else
		material.iridescenceThickness = iridescenceThicknessMaximum;
	#endif
#endif
#ifdef USE_SHEEN
	material.sheenColor = sheenColor;
	#ifdef USE_SHEEN_COLORMAP
		material.sheenColor *= texture2D( sheenColorMap, vSheenColorMapUv ).rgb;
	#endif
	material.sheenRoughness = clamp( sheenRoughness, 0.0001, 1.0 );
	#ifdef USE_SHEEN_ROUGHNESSMAP
		material.sheenRoughness *= texture2D( sheenRoughnessMap, vSheenRoughnessMapUv ).a;
	#endif
#endif
#ifdef USE_ANISOTROPY
	#ifdef USE_ANISOTROPYMAP
		mat2 anisotropyMat = mat2( anisotropyVector.x, anisotropyVector.y, - anisotropyVector.y, anisotropyVector.x );
		vec3 anisotropyPolar = texture2D( anisotropyMap, vAnisotropyMapUv ).rgb;
		vec2 anisotropyV = anisotropyMat * normalize( 2.0 * anisotropyPolar.rg - vec2( 1.0 ) ) * anisotropyPolar.b;
	#else
		vec2 anisotropyV = anisotropyVector;
	#endif
	material.anisotropy = length( anisotropyV );
	if( material.anisotropy == 0.0 ) {
		anisotropyV = vec2( 1.0, 0.0 );
	} else {
		anisotropyV /= material.anisotropy;
		material.anisotropy = saturate( material.anisotropy );
	}
	material.alphaT = mix( pow2( material.roughness ), 1.0, pow2( material.anisotropy ) );
	material.anisotropyT = tbn[ 0 ] * anisotropyV.x + tbn[ 1 ] * anisotropyV.y;
	material.anisotropyB = tbn[ 1 ] * anisotropyV.x - tbn[ 0 ] * anisotropyV.y;
#endif`,PT=`uniform sampler2D dfgLUT;
struct PhysicalMaterial {
	vec3 diffuseColor;
	vec3 diffuseContribution;
	vec3 specularColor;
	vec3 specularColorBlended;
	float roughness;
	float metalness;
	float specularF90;
	float dispersion;
	#ifdef USE_CLEARCOAT
		float clearcoat;
		float clearcoatRoughness;
		vec3 clearcoatF0;
		float clearcoatF90;
	#endif
	#ifdef USE_IRIDESCENCE
		float iridescence;
		float iridescenceIOR;
		float iridescenceThickness;
		vec3 iridescenceFresnel;
		vec3 iridescenceF0;
		vec3 iridescenceFresnelDielectric;
		vec3 iridescenceFresnelMetallic;
	#endif
	#ifdef USE_SHEEN
		vec3 sheenColor;
		float sheenRoughness;
	#endif
	#ifdef IOR
		float ior;
	#endif
	#ifdef USE_TRANSMISSION
		float transmission;
		float transmissionAlpha;
		float thickness;
		float attenuationDistance;
		vec3 attenuationColor;
	#endif
	#ifdef USE_ANISOTROPY
		float anisotropy;
		float alphaT;
		vec3 anisotropyT;
		vec3 anisotropyB;
	#endif
};
vec3 clearcoatSpecularDirect = vec3( 0.0 );
vec3 clearcoatSpecularIndirect = vec3( 0.0 );
vec3 sheenSpecularDirect = vec3( 0.0 );
vec3 sheenSpecularIndirect = vec3(0.0 );
vec3 Schlick_to_F0( const in vec3 f, const in float f90, const in float dotVH ) {
    float x = clamp( 1.0 - dotVH, 0.0, 1.0 );
    float x2 = x * x;
    float x5 = clamp( x * x2 * x2, 0.0, 0.9999 );
    return ( f - vec3( f90 ) * x5 ) / ( 1.0 - x5 );
}
float V_GGX_SmithCorrelated( const in float alpha, const in float dotNL, const in float dotNV ) {
	float a2 = pow2( alpha );
	float gv = dotNL * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNV ) );
	float gl = dotNV * sqrt( a2 + ( 1.0 - a2 ) * pow2( dotNL ) );
	return 0.5 / max( gv + gl, EPSILON );
}
float D_GGX( const in float alpha, const in float dotNH ) {
	float a2 = pow2( alpha );
	float denom = pow2( dotNH ) * ( a2 - 1.0 ) + 1.0;
	return RECIPROCAL_PI * a2 / pow2( denom );
}
#ifdef USE_ANISOTROPY
	float V_GGX_SmithCorrelated_Anisotropic( const in float alphaT, const in float alphaB, const in float dotTV, const in float dotBV, const in float dotTL, const in float dotBL, const in float dotNV, const in float dotNL ) {
		float gv = dotNL * length( vec3( alphaT * dotTV, alphaB * dotBV, dotNV ) );
		float gl = dotNV * length( vec3( alphaT * dotTL, alphaB * dotBL, dotNL ) );
		return 0.5 / max( gv + gl, EPSILON );
	}
	float D_GGX_Anisotropic( const in float alphaT, const in float alphaB, const in float dotNH, const in float dotTH, const in float dotBH ) {
		float a2 = alphaT * alphaB;
		highp vec3 v = vec3( alphaB * dotTH, alphaT * dotBH, a2 * dotNH );
		highp float v2 = dot( v, v );
		float w2 = a2 / v2;
		return RECIPROCAL_PI * a2 * pow2 ( w2 );
	}
#endif
#ifdef USE_CLEARCOAT
	vec3 BRDF_GGX_Clearcoat( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material) {
		vec3 f0 = material.clearcoatF0;
		float f90 = material.clearcoatF90;
		float roughness = material.clearcoatRoughness;
		float alpha = pow2( roughness );
		vec3 halfDir = normalize( lightDir + viewDir );
		float dotNL = saturate( dot( normal, lightDir ) );
		float dotNV = saturate( dot( normal, viewDir ) );
		float dotNH = saturate( dot( normal, halfDir ) );
		float dotVH = saturate( dot( viewDir, halfDir ) );
		vec3 F = F_Schlick( f0, f90, dotVH );
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
		return F * ( V * D );
	}
#endif
vec3 BRDF_GGX( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 f0 = material.specularColorBlended;
	float f90 = material.specularF90;
	float roughness = material.roughness;
	float alpha = pow2( roughness );
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float dotVH = saturate( dot( viewDir, halfDir ) );
	vec3 F = F_Schlick( f0, f90, dotVH );
	#ifdef USE_IRIDESCENCE
		F = mix( F, material.iridescenceFresnel, material.iridescence );
	#endif
	#ifdef USE_ANISOTROPY
		float dotTL = dot( material.anisotropyT, lightDir );
		float dotTV = dot( material.anisotropyT, viewDir );
		float dotTH = dot( material.anisotropyT, halfDir );
		float dotBL = dot( material.anisotropyB, lightDir );
		float dotBV = dot( material.anisotropyB, viewDir );
		float dotBH = dot( material.anisotropyB, halfDir );
		float V = V_GGX_SmithCorrelated_Anisotropic( material.alphaT, alpha, dotTV, dotBV, dotTL, dotBL, dotNV, dotNL );
		float D = D_GGX_Anisotropic( material.alphaT, alpha, dotNH, dotTH, dotBH );
	#else
		float V = V_GGX_SmithCorrelated( alpha, dotNL, dotNV );
		float D = D_GGX( alpha, dotNH );
	#endif
	return F * ( V * D );
}
vec2 LTC_Uv( const in vec3 N, const in vec3 V, const in float roughness ) {
	const float LUT_SIZE = 64.0;
	const float LUT_SCALE = ( LUT_SIZE - 1.0 ) / LUT_SIZE;
	const float LUT_BIAS = 0.5 / LUT_SIZE;
	float dotNV = saturate( dot( N, V ) );
	vec2 uv = vec2( roughness, sqrt( 1.0 - dotNV ) );
	uv = uv * LUT_SCALE + LUT_BIAS;
	return uv;
}
float LTC_ClippedSphereFormFactor( const in vec3 f ) {
	float l = length( f );
	return max( ( l * l + f.z ) / ( l + 1.0 ), 0.0 );
}
vec3 LTC_EdgeVectorFormFactor( const in vec3 v1, const in vec3 v2 ) {
	float x = dot( v1, v2 );
	float y = abs( x );
	float a = 0.8543985 + ( 0.4965155 + 0.0145206 * y ) * y;
	float b = 3.4175940 + ( 4.1616724 + y ) * y;
	float v = a / b;
	float theta_sintheta = ( x > 0.0 ) ? v : 0.5 * inversesqrt( max( 1.0 - x * x, 1e-7 ) ) - v;
	return cross( v1, v2 ) * theta_sintheta;
}
vec3 LTC_Evaluate( const in vec3 N, const in vec3 V, const in vec3 P, const in mat3 mInv, const in vec3 rectCoords[ 4 ] ) {
	vec3 v1 = rectCoords[ 1 ] - rectCoords[ 0 ];
	vec3 v2 = rectCoords[ 3 ] - rectCoords[ 0 ];
	vec3 lightNormal = cross( v1, v2 );
	if( dot( lightNormal, P - rectCoords[ 0 ] ) < 0.0 ) return vec3( 0.0 );
	vec3 T1, T2;
	T1 = normalize( V - N * dot( V, N ) );
	T2 = - cross( N, T1 );
	mat3 mat = mInv * transpose( mat3( T1, T2, N ) );
	vec3 coords[ 4 ];
	coords[ 0 ] = mat * ( rectCoords[ 0 ] - P );
	coords[ 1 ] = mat * ( rectCoords[ 1 ] - P );
	coords[ 2 ] = mat * ( rectCoords[ 2 ] - P );
	coords[ 3 ] = mat * ( rectCoords[ 3 ] - P );
	coords[ 0 ] = normalize( coords[ 0 ] );
	coords[ 1 ] = normalize( coords[ 1 ] );
	coords[ 2 ] = normalize( coords[ 2 ] );
	coords[ 3 ] = normalize( coords[ 3 ] );
	vec3 vectorFormFactor = vec3( 0.0 );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 0 ], coords[ 1 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 1 ], coords[ 2 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 2 ], coords[ 3 ] );
	vectorFormFactor += LTC_EdgeVectorFormFactor( coords[ 3 ], coords[ 0 ] );
	float result = LTC_ClippedSphereFormFactor( vectorFormFactor );
	return vec3( result );
}
#if defined( USE_SHEEN )
float D_Charlie( float roughness, float dotNH ) {
	float alpha = pow2( roughness );
	float invAlpha = 1.0 / alpha;
	float cos2h = dotNH * dotNH;
	float sin2h = max( 1.0 - cos2h, 0.0078125 );
	return ( 2.0 + invAlpha ) * pow( sin2h, invAlpha * 0.5 ) / ( 2.0 * PI );
}
float V_Neubelt( float dotNV, float dotNL ) {
	return saturate( 1.0 / ( 4.0 * ( dotNL + dotNV - dotNL * dotNV ) ) );
}
vec3 BRDF_Sheen( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, vec3 sheenColor, const in float sheenRoughness ) {
	vec3 halfDir = normalize( lightDir + viewDir );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	float dotNH = saturate( dot( normal, halfDir ) );
	float D = D_Charlie( sheenRoughness, dotNH );
	float V = V_Neubelt( dotNV, dotNL );
	return sheenColor * ( D * V );
}
#endif
float IBLSheenBRDF( const in vec3 normal, const in vec3 viewDir, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	float r2 = roughness * roughness;
	float rInv = 1.0 / ( roughness + 0.1 );
	float a = -1.9362 + 1.0678 * roughness + 0.4573 * r2 - 0.8469 * rInv;
	float b = -0.6014 + 0.5538 * roughness - 0.4670 * r2 - 0.1255 * rInv;
	float DG = exp( a * dotNV + b );
	return saturate( DG );
}
vec3 EnvironmentBRDF( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness ) {
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	return specularColor * fab.x + specularF90 * fab.y;
}
#ifdef USE_IRIDESCENCE
void computeMultiscatteringIridescence( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float iridescence, const in vec3 iridescenceF0, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#else
void computeMultiscattering( const in vec3 normal, const in vec3 viewDir, const in vec3 specularColor, const in float specularF90, const in float roughness, inout vec3 singleScatter, inout vec3 multiScatter ) {
#endif
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 fab = texture2D( dfgLUT, vec2( roughness, dotNV ) ).rg;
	#ifdef USE_IRIDESCENCE
		vec3 Fr = mix( specularColor, iridescenceF0, iridescence );
	#else
		vec3 Fr = specularColor;
	#endif
	vec3 FssEss = Fr * fab.x + specularF90 * fab.y;
	float Ess = fab.x + fab.y;
	float Ems = 1.0 - Ess;
	vec3 Favg = Fr + ( 1.0 - Fr ) * 0.047619;	vec3 Fms = FssEss * Favg / ( 1.0 - Ems * Favg );
	singleScatter += FssEss;
	multiScatter += Fms * Ems;
}
vec3 BRDF_GGX_Multiscatter( const in vec3 lightDir, const in vec3 viewDir, const in vec3 normal, const in PhysicalMaterial material ) {
	vec3 singleScatter = BRDF_GGX( lightDir, viewDir, normal, material );
	float dotNL = saturate( dot( normal, lightDir ) );
	float dotNV = saturate( dot( normal, viewDir ) );
	vec2 dfgV = texture2D( dfgLUT, vec2( material.roughness, dotNV ) ).rg;
	vec2 dfgL = texture2D( dfgLUT, vec2( material.roughness, dotNL ) ).rg;
	vec3 FssEss_V = material.specularColorBlended * dfgV.x + material.specularF90 * dfgV.y;
	vec3 FssEss_L = material.specularColorBlended * dfgL.x + material.specularF90 * dfgL.y;
	float Ess_V = dfgV.x + dfgV.y;
	float Ess_L = dfgL.x + dfgL.y;
	float Ems_V = 1.0 - Ess_V;
	float Ems_L = 1.0 - Ess_L;
	vec3 Favg = material.specularColorBlended + ( 1.0 - material.specularColorBlended ) * 0.047619;
	vec3 Fms = FssEss_V * FssEss_L * Favg / ( 1.0 - Ems_V * Ems_L * Favg + EPSILON );
	float compensationFactor = Ems_V * Ems_L;
	vec3 multiScatter = Fms * compensationFactor;
	return singleScatter + multiScatter;
}
#if NUM_RECT_AREA_LIGHTS > 0
	void RE_Direct_RectArea_Physical( const in RectAreaLight rectAreaLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
		vec3 normal = geometryNormal;
		vec3 viewDir = geometryViewDir;
		vec3 position = geometryPosition;
		vec3 lightPos = rectAreaLight.position;
		vec3 halfWidth = rectAreaLight.halfWidth;
		vec3 halfHeight = rectAreaLight.halfHeight;
		vec3 lightColor = rectAreaLight.color;
		float roughness = material.roughness;
		vec3 rectCoords[ 4 ];
		rectCoords[ 0 ] = lightPos + halfWidth - halfHeight;		rectCoords[ 1 ] = lightPos - halfWidth - halfHeight;
		rectCoords[ 2 ] = lightPos - halfWidth + halfHeight;
		rectCoords[ 3 ] = lightPos + halfWidth + halfHeight;
		vec2 uv = LTC_Uv( normal, viewDir, roughness );
		vec4 t1 = texture2D( ltc_1, uv );
		vec4 t2 = texture2D( ltc_2, uv );
		mat3 mInv = mat3(
			vec3( t1.x, 0, t1.y ),
			vec3(    0, 1,    0 ),
			vec3( t1.z, 0, t1.w )
		);
		vec3 fresnel = ( material.specularColorBlended * t2.x + ( material.specularF90 - material.specularColorBlended ) * t2.y );
		reflectedLight.directSpecular += lightColor * fresnel * LTC_Evaluate( normal, viewDir, position, mInv, rectCoords );
		reflectedLight.directDiffuse += lightColor * material.diffuseContribution * LTC_Evaluate( normal, viewDir, position, mat3( 1.0 ), rectCoords );
		#ifdef USE_CLEARCOAT
			vec3 Ncc = geometryClearcoatNormal;
			vec2 uvClearcoat = LTC_Uv( Ncc, viewDir, material.clearcoatRoughness );
			vec4 t1Clearcoat = texture2D( ltc_1, uvClearcoat );
			vec4 t2Clearcoat = texture2D( ltc_2, uvClearcoat );
			mat3 mInvClearcoat = mat3(
				vec3( t1Clearcoat.x, 0, t1Clearcoat.y ),
				vec3(             0, 1,             0 ),
				vec3( t1Clearcoat.z, 0, t1Clearcoat.w )
			);
			vec3 fresnelClearcoat = material.clearcoatF0 * t2Clearcoat.x + ( material.clearcoatF90 - material.clearcoatF0 ) * t2Clearcoat.y;
			clearcoatSpecularDirect += lightColor * fresnelClearcoat * LTC_Evaluate( Ncc, viewDir, position, mInvClearcoat, rectCoords );
		#endif
	}
#endif
void RE_Direct_Physical( const in IncidentLight directLight, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	float dotNL = saturate( dot( geometryNormal, directLight.direction ) );
	vec3 irradiance = dotNL * directLight.color;
	#ifdef USE_CLEARCOAT
		float dotNLcc = saturate( dot( geometryClearcoatNormal, directLight.direction ) );
		vec3 ccIrradiance = dotNLcc * directLight.color;
		clearcoatSpecularDirect += ccIrradiance * BRDF_GGX_Clearcoat( directLight.direction, geometryViewDir, geometryClearcoatNormal, material );
	#endif
	#ifdef USE_SHEEN
 
 		sheenSpecularDirect += irradiance * BRDF_Sheen( directLight.direction, geometryViewDir, geometryNormal, material.sheenColor, material.sheenRoughness );
 
 		float sheenAlbedoV = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
 		float sheenAlbedoL = IBLSheenBRDF( geometryNormal, directLight.direction, material.sheenRoughness );
 
 		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * max( sheenAlbedoV, sheenAlbedoL );
 
 		irradiance *= sheenEnergyComp;
 
 	#endif
	reflectedLight.directSpecular += irradiance * BRDF_GGX_Multiscatter( directLight.direction, geometryViewDir, geometryNormal, material );
	reflectedLight.directDiffuse += irradiance * BRDF_Lambert( material.diffuseContribution );
}
void RE_IndirectDiffuse_Physical( const in vec3 irradiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight ) {
	vec3 diffuse = irradiance * BRDF_Lambert( material.diffuseContribution );
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		diffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectDiffuse += diffuse;
}
void RE_IndirectSpecular_Physical( const in vec3 radiance, const in vec3 irradiance, const in vec3 clearcoatRadiance, const in vec3 geometryPosition, const in vec3 geometryNormal, const in vec3 geometryViewDir, const in vec3 geometryClearcoatNormal, const in PhysicalMaterial material, inout ReflectedLight reflectedLight) {
	#ifdef USE_CLEARCOAT
		clearcoatSpecularIndirect += clearcoatRadiance * EnvironmentBRDF( geometryClearcoatNormal, geometryViewDir, material.clearcoatF0, material.clearcoatF90, material.clearcoatRoughness );
	#endif
	#ifdef USE_SHEEN
		sheenSpecularIndirect += irradiance * material.sheenColor * IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness ) * RECIPROCAL_PI;
 	#endif
	vec3 singleScatteringDielectric = vec3( 0.0 );
	vec3 multiScatteringDielectric = vec3( 0.0 );
	vec3 singleScatteringMetallic = vec3( 0.0 );
	vec3 multiScatteringMetallic = vec3( 0.0 );
	#ifdef USE_IRIDESCENCE
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.iridescence, material.iridescenceFresnelDielectric, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscatteringIridescence( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.iridescence, material.iridescenceFresnelMetallic, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#else
		computeMultiscattering( geometryNormal, geometryViewDir, material.specularColor, material.specularF90, material.roughness, singleScatteringDielectric, multiScatteringDielectric );
		computeMultiscattering( geometryNormal, geometryViewDir, material.diffuseColor, material.specularF90, material.roughness, singleScatteringMetallic, multiScatteringMetallic );
	#endif
	vec3 singleScattering = mix( singleScatteringDielectric, singleScatteringMetallic, material.metalness );
	vec3 multiScattering = mix( multiScatteringDielectric, multiScatteringMetallic, material.metalness );
	vec3 totalScatteringDielectric = singleScatteringDielectric + multiScatteringDielectric;
	vec3 diffuse = material.diffuseContribution * ( 1.0 - totalScatteringDielectric );
	vec3 cosineWeightedIrradiance = irradiance * RECIPROCAL_PI;
	vec3 indirectSpecular = radiance * singleScattering;
	indirectSpecular += multiScattering * cosineWeightedIrradiance;
	vec3 indirectDiffuse = diffuse * cosineWeightedIrradiance;
	#ifdef USE_SHEEN
		float sheenAlbedo = IBLSheenBRDF( geometryNormal, geometryViewDir, material.sheenRoughness );
		float sheenEnergyComp = 1.0 - max3( material.sheenColor ) * sheenAlbedo;
		indirectSpecular *= sheenEnergyComp;
		indirectDiffuse *= sheenEnergyComp;
	#endif
	reflectedLight.indirectSpecular += indirectSpecular;
	reflectedLight.indirectDiffuse += indirectDiffuse;
}
#define RE_Direct				RE_Direct_Physical
#define RE_Direct_RectArea		RE_Direct_RectArea_Physical
#define RE_IndirectDiffuse		RE_IndirectDiffuse_Physical
#define RE_IndirectSpecular		RE_IndirectSpecular_Physical
float computeSpecularOcclusion( const in float dotNV, const in float ambientOcclusion, const in float roughness ) {
	return saturate( pow( dotNV + ambientOcclusion, exp2( - 16.0 * roughness - 1.0 ) ) - 1.0 + ambientOcclusion );
}`,LT=`
vec3 geometryPosition = - vViewPosition;
vec3 geometryNormal = normal;
vec3 geometryViewDir = ( isOrthographic ) ? vec3( 0, 0, 1 ) : normalize( vViewPosition );
vec3 geometryClearcoatNormal = vec3( 0.0 );
#ifdef USE_CLEARCOAT
	geometryClearcoatNormal = clearcoatNormal;
#endif
#ifdef USE_IRIDESCENCE
	float dotNVi = saturate( dot( normal, geometryViewDir ) );
	if ( material.iridescenceThickness == 0.0 ) {
		material.iridescence = 0.0;
	} else {
		material.iridescence = saturate( material.iridescence );
	}
	if ( material.iridescence > 0.0 ) {
		material.iridescenceFresnelDielectric = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.specularColor );
		material.iridescenceFresnelMetallic = evalIridescence( 1.0, material.iridescenceIOR, dotNVi, material.iridescenceThickness, material.diffuseColor );
		material.iridescenceFresnel = mix( material.iridescenceFresnelDielectric, material.iridescenceFresnelMetallic, material.metalness );
		material.iridescenceF0 = Schlick_to_F0( material.iridescenceFresnel, 1.0, dotNVi );
	}
#endif
IncidentLight directLight;
#if ( NUM_POINT_LIGHTS > 0 ) && defined( RE_Direct )
	PointLight pointLight;
	#if defined( USE_SHADOWMAP ) && NUM_POINT_LIGHT_SHADOWS > 0
	PointLightShadow pointLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHTS; i ++ ) {
		pointLight = pointLights[ i ];
		getPointLightInfo( pointLight, geometryPosition, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_POINT_LIGHT_SHADOWS ) && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
		pointLightShadow = pointLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getPointShadow( pointShadowMap[ i ], pointLightShadow.shadowMapSize, pointLightShadow.shadowIntensity, pointLightShadow.shadowBias, pointLightShadow.shadowRadius, vPointShadowCoord[ i ], pointLightShadow.shadowCameraNear, pointLightShadow.shadowCameraFar ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_SPOT_LIGHTS > 0 ) && defined( RE_Direct )
	SpotLight spotLight;
	vec4 spotColor;
	vec3 spotLightCoord;
	bool inSpotLightMap;
	#if defined( USE_SHADOWMAP ) && NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHTS; i ++ ) {
		spotLight = spotLights[ i ];
		getSpotLightInfo( spotLight, geometryPosition, directLight );
		#if ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#define SPOT_LIGHT_MAP_INDEX UNROLLED_LOOP_INDEX
		#elif ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		#define SPOT_LIGHT_MAP_INDEX NUM_SPOT_LIGHT_MAPS
		#else
		#define SPOT_LIGHT_MAP_INDEX ( UNROLLED_LOOP_INDEX - NUM_SPOT_LIGHT_SHADOWS + NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS )
		#endif
		#if ( SPOT_LIGHT_MAP_INDEX < NUM_SPOT_LIGHT_MAPS )
			spotLightCoord = vSpotLightCoord[ i ].xyz / vSpotLightCoord[ i ].w;
			inSpotLightMap = all( lessThan( abs( spotLightCoord * 2. - 1. ), vec3( 1.0 ) ) );
			spotColor = texture2D( spotLightMap[ SPOT_LIGHT_MAP_INDEX ], spotLightCoord.xy );
			directLight.color = inSpotLightMap ? directLight.color * spotColor.rgb : directLight.color;
		#endif
		#undef SPOT_LIGHT_MAP_INDEX
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
		spotLightShadow = spotLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( spotShadowMap[ i ], spotLightShadow.shadowMapSize, spotLightShadow.shadowIntensity, spotLightShadow.shadowBias, spotLightShadow.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_DIR_LIGHTS > 0 ) && defined( RE_Direct )
	DirectionalLight directionalLight;
	#if defined( USE_SHADOWMAP ) && NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLightShadow;
	#endif
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHTS; i ++ ) {
		directionalLight = directionalLights[ i ];
		getDirectionalLightInfo( directionalLight, directLight );
		#if defined( USE_SHADOWMAP ) && ( UNROLLED_LOOP_INDEX < NUM_DIR_LIGHT_SHADOWS )
		directionalLightShadow = directionalLightShadows[ i ];
		directLight.color *= ( directLight.visible && receiveShadow ) ? getShadow( directionalShadowMap[ i ], directionalLightShadow.shadowMapSize, directionalLightShadow.shadowIntensity, directionalLightShadow.shadowBias, directionalLightShadow.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
		#endif
		RE_Direct( directLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if ( NUM_RECT_AREA_LIGHTS > 0 ) && defined( RE_Direct_RectArea )
	RectAreaLight rectAreaLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_RECT_AREA_LIGHTS; i ++ ) {
		rectAreaLight = rectAreaLights[ i ];
		RE_Direct_RectArea( rectAreaLight, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
	}
	#pragma unroll_loop_end
#endif
#if defined( RE_IndirectDiffuse )
	vec3 iblIrradiance = vec3( 0.0 );
	vec3 irradiance = getAmbientLightIrradiance( ambientLightColor );
	#if defined( USE_LIGHT_PROBES )
		irradiance += getLightProbeIrradiance( lightProbe, geometryNormal );
	#endif
	#if ( NUM_HEMI_LIGHTS > 0 )
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_HEMI_LIGHTS; i ++ ) {
			irradiance += getHemisphereLightIrradiance( hemisphereLights[ i ], geometryNormal );
		}
		#pragma unroll_loop_end
	#endif
	#ifdef USE_LIGHT_PROBES_GRID
		vec3 probeWorldPos = ( ( vec4( geometryPosition, 1.0 ) - viewMatrix[ 3 ] ) * viewMatrix ).xyz;
		vec3 probeWorldNormal = inverseTransformDirection( geometryNormal, viewMatrix );
		irradiance += getLightProbeGridIrradiance( probeWorldPos, probeWorldNormal );
	#endif
#endif
#if defined( RE_IndirectSpecular )
	vec3 radiance = vec3( 0.0 );
	vec3 clearcoatRadiance = vec3( 0.0 );
#endif`,OT=`#if defined( RE_IndirectDiffuse )
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		vec3 lightMapIrradiance = lightMapTexel.rgb * lightMapIntensity;
		irradiance += lightMapIrradiance;
	#endif
	#if defined( USE_ENVMAP ) && defined( ENVMAP_TYPE_CUBE_UV )
		#if defined( STANDARD ) || defined( LAMBERT ) || defined( PHONG )
			iblIrradiance += getIBLIrradiance( geometryNormal );
		#endif
	#endif
#endif
#if defined( USE_ENVMAP ) && defined( RE_IndirectSpecular )
	#ifdef USE_ANISOTROPY
		radiance += getIBLAnisotropyRadiance( geometryViewDir, geometryNormal, material.roughness, material.anisotropyB, material.anisotropy );
	#else
		radiance += getIBLRadiance( geometryViewDir, geometryNormal, material.roughness );
	#endif
	#ifdef USE_CLEARCOAT
		clearcoatRadiance += getIBLRadiance( geometryViewDir, geometryClearcoatNormal, material.clearcoatRoughness );
	#endif
#endif`,FT=`#if defined( RE_IndirectDiffuse )
	#if defined( LAMBERT ) || defined( PHONG )
		irradiance += iblIrradiance;
	#endif
	RE_IndirectDiffuse( irradiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif
#if defined( RE_IndirectSpecular )
	RE_IndirectSpecular( radiance, iblIrradiance, clearcoatRadiance, geometryPosition, geometryNormal, geometryViewDir, geometryClearcoatNormal, material, reflectedLight );
#endif`,kT=`#ifdef USE_LIGHT_PROBES_GRID
uniform highp sampler3D probesSH;
uniform vec3 probesMin;
uniform vec3 probesMax;
uniform vec3 probesResolution;
vec3 getLightProbeGridIrradiance( vec3 worldPos, vec3 worldNormal ) {
	vec3 res = probesResolution;
	vec3 gridRange = probesMax - probesMin;
	vec3 resMinusOne = res - 1.0;
	vec3 probeSpacing = gridRange / resMinusOne;
	vec3 samplePos = worldPos + worldNormal * probeSpacing * 0.5;
	vec3 uvw = clamp( ( samplePos - probesMin ) / gridRange, 0.0, 1.0 );
	uvw = uvw * resMinusOne / res + 0.5 / res;
	float nz          = res.z;
	float paddedSlices = nz + 2.0;
	float atlasDepth  = 7.0 * paddedSlices;
	float uvZBase     = uvw.z * nz + 1.0;
	vec4 s0 = texture( probesSH, vec3( uvw.xy, ( uvZBase                       ) / atlasDepth ) );
	vec4 s1 = texture( probesSH, vec3( uvw.xy, ( uvZBase +       paddedSlices   ) / atlasDepth ) );
	vec4 s2 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 2.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s3 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 3.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s4 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 4.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s5 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 5.0 * paddedSlices   ) / atlasDepth ) );
	vec4 s6 = texture( probesSH, vec3( uvw.xy, ( uvZBase + 6.0 * paddedSlices   ) / atlasDepth ) );
	vec3 c0 = s0.xyz;
	vec3 c1 = vec3( s0.w, s1.xy );
	vec3 c2 = vec3( s1.zw, s2.x );
	vec3 c3 = s2.yzw;
	vec3 c4 = s3.xyz;
	vec3 c5 = vec3( s3.w, s4.xy );
	vec3 c6 = vec3( s4.zw, s5.x );
	vec3 c7 = s5.yzw;
	vec3 c8 = s6.xyz;
	float x = worldNormal.x, y = worldNormal.y, z = worldNormal.z;
	vec3 result = c0 * 0.886227;
	result += c1 * 2.0 * 0.511664 * y;
	result += c2 * 2.0 * 0.511664 * z;
	result += c3 * 2.0 * 0.511664 * x;
	result += c4 * 2.0 * 0.429043 * x * y;
	result += c5 * 2.0 * 0.429043 * y * z;
	result += c6 * ( 0.743125 * z * z - 0.247708 );
	result += c7 * 2.0 * 0.429043 * x * z;
	result += c8 * 0.429043 * ( x * x - y * y );
	return max( result, vec3( 0.0 ) );
}
#endif`,UT=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	gl_FragDepth = vIsPerspective == 0.0 ? gl_FragCoord.z : log2( vFragDepth ) * logDepthBufFC * 0.5;
#endif`,BT=`#if defined( USE_LOGARITHMIC_DEPTH_BUFFER )
	uniform float logDepthBufFC;
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,VT=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	varying float vFragDepth;
	varying float vIsPerspective;
#endif`,HT=`#ifdef USE_LOGARITHMIC_DEPTH_BUFFER
	vFragDepth = 1.0 + gl_Position.w;
	vIsPerspective = float( isPerspectiveMatrix( projectionMatrix ) );
#endif`,zT=`#ifdef USE_MAP
	vec4 sampledDiffuseColor = texture2D( map, vMapUv );
	#ifdef DECODE_VIDEO_TEXTURE
		sampledDiffuseColor = sRGBTransferEOTF( sampledDiffuseColor );
	#endif
	diffuseColor *= sampledDiffuseColor;
#endif`,GT=`#ifdef USE_MAP
	uniform sampler2D map;
#endif`,WT=`#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
	#if defined( USE_POINTS_UV )
		vec2 uv = vUv;
	#else
		vec2 uv = ( uvTransform * vec3( gl_PointCoord.x, 1.0 - gl_PointCoord.y, 1 ) ).xy;
	#endif
#endif
#ifdef USE_MAP
	diffuseColor *= texture2D( map, uv );
#endif
#ifdef USE_ALPHAMAP
	diffuseColor.a *= texture2D( alphaMap, uv ).g;
#endif`,jT=`#if defined( USE_POINTS_UV )
	varying vec2 vUv;
#else
	#if defined( USE_MAP ) || defined( USE_ALPHAMAP )
		uniform mat3 uvTransform;
	#endif
#endif
#ifdef USE_MAP
	uniform sampler2D map;
#endif
#ifdef USE_ALPHAMAP
	uniform sampler2D alphaMap;
#endif`,$T=`float metalnessFactor = metalness;
#ifdef USE_METALNESSMAP
	vec4 texelMetalness = texture2D( metalnessMap, vMetalnessMapUv );
	metalnessFactor *= texelMetalness.b;
#endif`,qT=`#ifdef USE_METALNESSMAP
	uniform sampler2D metalnessMap;
#endif`,XT=`#ifdef USE_INSTANCING_MORPH
	float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	float morphTargetBaseInfluence = texelFetch( morphTexture, ivec2( 0, gl_InstanceID ), 0 ).r;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		morphTargetInfluences[i] =  texelFetch( morphTexture, ivec2( i + 1, gl_InstanceID ), 0 ).r;
	}
#endif`,YT=`#if defined( USE_MORPHCOLORS )
	vColor *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		#if defined( USE_COLOR_ALPHA )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ) * morphTargetInfluences[ i ];
		#elif defined( USE_COLOR )
			if ( morphTargetInfluences[ i ] != 0.0 ) vColor += getMorph( gl_VertexID, i, 2 ).rgb * morphTargetInfluences[ i ];
		#endif
	}
#endif`,ZT=`#ifdef USE_MORPHNORMALS
	objectNormal *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) objectNormal += getMorph( gl_VertexID, i, 1 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,JT=`#ifdef USE_MORPHTARGETS
	#ifndef USE_INSTANCING_MORPH
		uniform float morphTargetBaseInfluence;
		uniform float morphTargetInfluences[ MORPHTARGETS_COUNT ];
	#endif
	uniform sampler2DArray morphTargetsTexture;
	uniform ivec2 morphTargetsTextureSize;
	vec4 getMorph( const in int vertexIndex, const in int morphTargetIndex, const in int offset ) {
		int texelIndex = vertexIndex * MORPHTARGETS_TEXTURE_STRIDE + offset;
		int y = texelIndex / morphTargetsTextureSize.x;
		int x = texelIndex - y * morphTargetsTextureSize.x;
		ivec3 morphUV = ivec3( x, y, morphTargetIndex );
		return texelFetch( morphTargetsTexture, morphUV, 0 );
	}
#endif`,KT=`#ifdef USE_MORPHTARGETS
	transformed *= morphTargetBaseInfluence;
	for ( int i = 0; i < MORPHTARGETS_COUNT; i ++ ) {
		if ( morphTargetInfluences[ i ] != 0.0 ) transformed += getMorph( gl_VertexID, i, 0 ).xyz * morphTargetInfluences[ i ];
	}
#endif`,QT=`float faceDirection = gl_FrontFacing ? 1.0 : - 1.0;
#ifdef FLAT_SHADED
	vec3 fdx = dFdx( vViewPosition );
	vec3 fdy = dFdy( vViewPosition );
	vec3 normal = normalize( cross( fdx, fdy ) );
#else
	vec3 normal = normalize( vNormal );
	#ifdef DOUBLE_SIDED
		normal *= faceDirection;
	#endif
#endif
#if defined( USE_NORMALMAP_TANGENTSPACE ) || defined( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY )
	#ifdef USE_TANGENT
		mat3 tbn = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn = getTangentFrame( - vViewPosition, normal,
		#if defined( USE_NORMALMAP )
			vNormalMapUv
		#elif defined( USE_CLEARCOAT_NORMALMAP )
			vClearcoatNormalMapUv
		#else
			vUv
		#endif
		);
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn[0] *= faceDirection;
		tbn[1] *= faceDirection;
	#endif
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	#ifdef USE_TANGENT
		mat3 tbn2 = mat3( normalize( vTangent ), normalize( vBitangent ), normal );
	#else
		mat3 tbn2 = getTangentFrame( - vViewPosition, normal, vClearcoatNormalMapUv );
	#endif
	#if defined( DOUBLE_SIDED ) && ! defined( FLAT_SHADED )
		tbn2[0] *= faceDirection;
		tbn2[1] *= faceDirection;
	#endif
#endif
vec3 nonPerturbedNormal = normal;`,ew=`#ifdef USE_NORMALMAP_OBJECTSPACE
	normal = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#ifdef FLIP_SIDED
		normal = - normal;
	#endif
	#ifdef DOUBLE_SIDED
		normal = normal * faceDirection;
	#endif
	normal = normalize( normalMatrix * normal );
#elif defined( USE_NORMALMAP_TANGENTSPACE )
	vec3 mapN = texture2D( normalMap, vNormalMapUv ).xyz * 2.0 - 1.0;
	#if defined( USE_PACKED_NORMALMAP )
		mapN = vec3( mapN.xy, sqrt( saturate( 1.0 - dot( mapN.xy, mapN.xy ) ) ) );
	#endif
	mapN.xy *= normalScale;
	normal = normalize( tbn * mapN );
#elif defined( USE_BUMPMAP )
	normal = perturbNormalArb( - vViewPosition, normal, dHdxy_fwd(), faceDirection );
#endif`,tw=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,nw=`#ifndef FLAT_SHADED
	varying vec3 vNormal;
	#ifdef USE_TANGENT
		varying vec3 vTangent;
		varying vec3 vBitangent;
	#endif
#endif`,iw=`#ifndef FLAT_SHADED
	vNormal = normalize( transformedNormal );
	#ifdef USE_TANGENT
		vTangent = normalize( transformedTangent );
		vBitangent = normalize( cross( vNormal, vTangent ) * tangent.w );
	#endif
#endif`,rw=`#ifdef USE_NORMALMAP
	uniform sampler2D normalMap;
	uniform vec2 normalScale;
#endif
#ifdef USE_NORMALMAP_OBJECTSPACE
	uniform mat3 normalMatrix;
#endif
#if ! defined ( USE_TANGENT ) && ( defined ( USE_NORMALMAP_TANGENTSPACE ) || defined ( USE_CLEARCOAT_NORMALMAP ) || defined( USE_ANISOTROPY ) )
	mat3 getTangentFrame( vec3 eye_pos, vec3 surf_norm, vec2 uv ) {
		vec3 q0 = dFdx( eye_pos.xyz );
		vec3 q1 = dFdy( eye_pos.xyz );
		vec2 st0 = dFdx( uv.st );
		vec2 st1 = dFdy( uv.st );
		vec3 N = surf_norm;
		vec3 q1perp = cross( q1, N );
		vec3 q0perp = cross( N, q0 );
		vec3 T = q1perp * st0.x + q0perp * st1.x;
		vec3 B = q1perp * st0.y + q0perp * st1.y;
		float det = max( dot( T, T ), dot( B, B ) );
		float scale = ( det == 0.0 ) ? 0.0 : inversesqrt( det );
		return mat3( T * scale, B * scale, N );
	}
#endif`,sw=`#ifdef USE_CLEARCOAT
	vec3 clearcoatNormal = nonPerturbedNormal;
#endif`,ow=`#ifdef USE_CLEARCOAT_NORMALMAP
	vec3 clearcoatMapN = texture2D( clearcoatNormalMap, vClearcoatNormalMapUv ).xyz * 2.0 - 1.0;
	clearcoatMapN.xy *= clearcoatNormalScale;
	clearcoatNormal = normalize( tbn2 * clearcoatMapN );
#endif`,aw=`#ifdef USE_CLEARCOATMAP
	uniform sampler2D clearcoatMap;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform sampler2D clearcoatNormalMap;
	uniform vec2 clearcoatNormalScale;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform sampler2D clearcoatRoughnessMap;
#endif`,cw=`#ifdef USE_IRIDESCENCEMAP
	uniform sampler2D iridescenceMap;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform sampler2D iridescenceThicknessMap;
#endif`,lw=`#ifdef OPAQUE
diffuseColor.a = 1.0;
#endif
#ifdef USE_TRANSMISSION
diffuseColor.a *= material.transmissionAlpha;
#endif
gl_FragColor = vec4( outgoingLight, diffuseColor.a );`,uw=`vec3 packNormalToRGB( const in vec3 normal ) {
	return normalize( normal ) * 0.5 + 0.5;
}
vec3 unpackRGBToNormal( const in vec3 rgb ) {
	return 2.0 * rgb.xyz - 1.0;
}
const float PackUpscale = 256. / 255.;const float UnpackDownscale = 255. / 256.;const float ShiftRight8 = 1. / 256.;
const float Inv255 = 1. / 255.;
const vec4 PackFactors = vec4( 1.0, 256.0, 256.0 * 256.0, 256.0 * 256.0 * 256.0 );
const vec2 UnpackFactors2 = vec2( UnpackDownscale, 1.0 / PackFactors.g );
const vec3 UnpackFactors3 = vec3( UnpackDownscale / PackFactors.rg, 1.0 / PackFactors.b );
const vec4 UnpackFactors4 = vec4( UnpackDownscale / PackFactors.rgb, 1.0 / PackFactors.a );
vec4 packDepthToRGBA( const in float v ) {
	if( v <= 0.0 )
		return vec4( 0., 0., 0., 0. );
	if( v >= 1.0 )
		return vec4( 1., 1., 1., 1. );
	float vuf;
	float af = modf( v * PackFactors.a, vuf );
	float bf = modf( vuf * ShiftRight8, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec4( vuf * Inv255, gf * PackUpscale, bf * PackUpscale, af );
}
vec3 packDepthToRGB( const in float v ) {
	if( v <= 0.0 )
		return vec3( 0., 0., 0. );
	if( v >= 1.0 )
		return vec3( 1., 1., 1. );
	float vuf;
	float bf = modf( v * PackFactors.b, vuf );
	float gf = modf( vuf * ShiftRight8, vuf );
	return vec3( vuf * Inv255, gf * PackUpscale, bf );
}
vec2 packDepthToRG( const in float v ) {
	if( v <= 0.0 )
		return vec2( 0., 0. );
	if( v >= 1.0 )
		return vec2( 1., 1. );
	float vuf;
	float gf = modf( v * 256., vuf );
	return vec2( vuf * Inv255, gf );
}
float unpackRGBAToDepth( const in vec4 v ) {
	return dot( v, UnpackFactors4 );
}
float unpackRGBToDepth( const in vec3 v ) {
	return dot( v, UnpackFactors3 );
}
float unpackRGToDepth( const in vec2 v ) {
	return v.r * UnpackFactors2.r + v.g * UnpackFactors2.g;
}
vec4 pack2HalfToRGBA( const in vec2 v ) {
	vec4 r = vec4( v.x, fract( v.x * 255.0 ), v.y, fract( v.y * 255.0 ) );
	return vec4( r.x - r.y / 255.0, r.y, r.z - r.w / 255.0, r.w );
}
vec2 unpackRGBATo2Half( const in vec4 v ) {
	return vec2( v.x + ( v.y / 255.0 ), v.z + ( v.w / 255.0 ) );
}
float viewZToOrthographicDepth( const in float viewZ, const in float near, const in float far ) {
	return ( viewZ + near ) / ( near - far );
}
float orthographicDepthToViewZ( const in float depth, const in float near, const in float far ) {
	#ifdef USE_REVERSED_DEPTH_BUFFER
	
		return depth * ( far - near ) - far;
	#else
		return depth * ( near - far ) - near;
	#endif
}
float viewZToPerspectiveDepth( const in float viewZ, const in float near, const in float far ) {
	return ( ( near + viewZ ) * far ) / ( ( far - near ) * viewZ );
}
float perspectiveDepthToViewZ( const in float depth, const in float near, const in float far ) {
	
	#ifdef USE_REVERSED_DEPTH_BUFFER
		return ( near * far ) / ( ( near - far ) * depth - near );
	#else
		return ( near * far ) / ( ( far - near ) * depth - far );
	#endif
}`,dw=`#ifdef PREMULTIPLIED_ALPHA
	gl_FragColor.rgb *= gl_FragColor.a;
#endif`,fw=`vec4 mvPosition = vec4( transformed, 1.0 );
#ifdef USE_BATCHING
	mvPosition = batchingMatrix * mvPosition;
#endif
#ifdef USE_INSTANCING
	mvPosition = instanceMatrix * mvPosition;
#endif
mvPosition = modelViewMatrix * mvPosition;
gl_Position = projectionMatrix * mvPosition;`,hw=`#ifdef DITHERING
	gl_FragColor.rgb = dithering( gl_FragColor.rgb );
#endif`,pw=`#ifdef DITHERING
	vec3 dithering( vec3 color ) {
		float grid_position = rand( gl_FragCoord.xy );
		vec3 dither_shift_RGB = vec3( 0.25 / 255.0, -0.25 / 255.0, 0.25 / 255.0 );
		dither_shift_RGB = mix( 2.0 * dither_shift_RGB, -2.0 * dither_shift_RGB, grid_position );
		return color + dither_shift_RGB;
	}
#endif`,mw=`float roughnessFactor = roughness;
#ifdef USE_ROUGHNESSMAP
	vec4 texelRoughness = texture2D( roughnessMap, vRoughnessMapUv );
	roughnessFactor *= texelRoughness.g;
#endif`,gw=`#ifdef USE_ROUGHNESSMAP
	uniform sampler2D roughnessMap;
#endif`,vw=`#if NUM_SPOT_LIGHT_COORDS > 0
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#if NUM_SPOT_LIGHT_MAPS > 0
	uniform sampler2D spotLightMap[ NUM_SPOT_LIGHT_MAPS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#else
			uniform sampler2D directionalShadowMap[ NUM_DIR_LIGHT_SHADOWS ];
		#endif
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform sampler2DShadow spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#else
			uniform sampler2D spotShadowMap[ NUM_SPOT_LIGHT_SHADOWS ];
		#endif
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#if defined( SHADOWMAP_TYPE_PCF )
			uniform samplerCubeShadow pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#elif defined( SHADOWMAP_TYPE_BASIC )
			uniform samplerCube pointShadowMap[ NUM_POINT_LIGHT_SHADOWS ];
		#endif
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float interleavedGradientNoise( vec2 position ) {
			return fract( 52.9829189 * fract( dot( position, vec2( 0.06711056, 0.00583715 ) ) ) );
		}
		vec2 vogelDiskSample( int sampleIndex, int samplesCount, float phi ) {
			const float goldenAngle = 2.399963229728653;
			float r = sqrt( ( float( sampleIndex ) + 0.5 ) / float( samplesCount ) );
			float theta = float( sampleIndex ) * goldenAngle + phi;
			return vec2( cos( theta ), sin( theta ) ) * r;
		}
	#endif
	#if defined( SHADOWMAP_TYPE_PCF )
		float getShadow( sampler2DShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			shadowCoord.z += shadowBias;
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 texelSize = vec2( 1.0 ) / shadowMapSize;
				float radius = shadowRadius * texelSize.x;
				float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
				shadow = (
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 0, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 1, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 2, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 3, 5, phi ) * radius, shadowCoord.z ) ) +
					texture( shadowMap, vec3( shadowCoord.xy + vogelDiskSample( 4, 5, phi ) * radius, shadowCoord.z ) )
				) * 0.2;
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#elif defined( SHADOWMAP_TYPE_VSM )
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				vec2 distribution = texture2D( shadowMap, shadowCoord.xy ).rg;
				float mean = distribution.x;
				float variance = distribution.y * distribution.y;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					float hard_shadow = step( mean, shadowCoord.z );
				#else
					float hard_shadow = step( shadowCoord.z, mean );
				#endif
				
				if ( hard_shadow == 1.0 ) {
					shadow = 1.0;
				} else {
					variance = max( variance, 0.0000001 );
					float d = shadowCoord.z - mean;
					float p_max = variance / ( variance + d * d );
					p_max = clamp( ( p_max - 0.3 ) / 0.65, 0.0, 1.0 );
					shadow = max( hard_shadow, p_max );
				}
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#else
		float getShadow( sampler2D shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord ) {
			float shadow = 1.0;
			shadowCoord.xyz /= shadowCoord.w;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				shadowCoord.z -= shadowBias;
			#else
				shadowCoord.z += shadowBias;
			#endif
			bool inFrustum = shadowCoord.x >= 0.0 && shadowCoord.x <= 1.0 && shadowCoord.y >= 0.0 && shadowCoord.y <= 1.0;
			bool frustumTest = inFrustum && shadowCoord.z <= 1.0;
			if ( frustumTest ) {
				float depth = texture2D( shadowMap, shadowCoord.xy ).r;
				#ifdef USE_REVERSED_DEPTH_BUFFER
					shadow = step( depth, shadowCoord.z );
				#else
					shadow = step( shadowCoord.z, depth );
				#endif
			}
			return mix( 1.0, shadow, shadowIntensity );
		}
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
	#if defined( SHADOWMAP_TYPE_PCF )
	float getPointShadow( samplerCubeShadow shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 bd3D = normalize( lightToPosition );
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			#ifdef USE_REVERSED_DEPTH_BUFFER
				float dp = ( shadowCameraNear * ( shadowCameraFar - viewSpaceZ ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp -= shadowBias;
			#else
				float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
				dp += shadowBias;
			#endif
			float texelSize = shadowRadius / shadowMapSize.x;
			vec3 absDir = abs( bd3D );
			vec3 tangent = absDir.x > absDir.z ? vec3( 0.0, 1.0, 0.0 ) : vec3( 1.0, 0.0, 0.0 );
			tangent = normalize( cross( bd3D, tangent ) );
			vec3 bitangent = cross( bd3D, tangent );
			float phi = interleavedGradientNoise( gl_FragCoord.xy ) * PI2;
			vec2 sample0 = vogelDiskSample( 0, 5, phi );
			vec2 sample1 = vogelDiskSample( 1, 5, phi );
			vec2 sample2 = vogelDiskSample( 2, 5, phi );
			vec2 sample3 = vogelDiskSample( 3, 5, phi );
			vec2 sample4 = vogelDiskSample( 4, 5, phi );
			shadow = (
				texture( shadowMap, vec4( bd3D + ( tangent * sample0.x + bitangent * sample0.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample1.x + bitangent * sample1.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample2.x + bitangent * sample2.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample3.x + bitangent * sample3.y ) * texelSize, dp ) ) +
				texture( shadowMap, vec4( bd3D + ( tangent * sample4.x + bitangent * sample4.y ) * texelSize, dp ) )
			) * 0.2;
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#elif defined( SHADOWMAP_TYPE_BASIC )
	float getPointShadow( samplerCube shadowMap, vec2 shadowMapSize, float shadowIntensity, float shadowBias, float shadowRadius, vec4 shadowCoord, float shadowCameraNear, float shadowCameraFar ) {
		float shadow = 1.0;
		vec3 lightToPosition = shadowCoord.xyz;
		vec3 absVec = abs( lightToPosition );
		float viewSpaceZ = max( max( absVec.x, absVec.y ), absVec.z );
		if ( viewSpaceZ - shadowCameraFar <= 0.0 && viewSpaceZ - shadowCameraNear >= 0.0 ) {
			float dp = ( shadowCameraFar * ( viewSpaceZ - shadowCameraNear ) ) / ( viewSpaceZ * ( shadowCameraFar - shadowCameraNear ) );
			dp += shadowBias;
			vec3 bd3D = normalize( lightToPosition );
			float depth = textureCube( shadowMap, bd3D ).r;
			#ifdef USE_REVERSED_DEPTH_BUFFER
				depth = 1.0 - depth;
			#endif
			shadow = step( dp, depth );
		}
		return mix( 1.0, shadow, shadowIntensity );
	}
	#endif
	#endif
#endif`,yw=`#if NUM_SPOT_LIGHT_COORDS > 0
	uniform mat4 spotLightMatrix[ NUM_SPOT_LIGHT_COORDS ];
	varying vec4 vSpotLightCoord[ NUM_SPOT_LIGHT_COORDS ];
#endif
#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
		uniform mat4 directionalShadowMatrix[ NUM_DIR_LIGHT_SHADOWS ];
		varying vec4 vDirectionalShadowCoord[ NUM_DIR_LIGHT_SHADOWS ];
		struct DirectionalLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform DirectionalLightShadow directionalLightShadows[ NUM_DIR_LIGHT_SHADOWS ];
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
		struct SpotLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
		};
		uniform SpotLightShadow spotLightShadows[ NUM_SPOT_LIGHT_SHADOWS ];
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		uniform mat4 pointShadowMatrix[ NUM_POINT_LIGHT_SHADOWS ];
		varying vec4 vPointShadowCoord[ NUM_POINT_LIGHT_SHADOWS ];
		struct PointLightShadow {
			float shadowIntensity;
			float shadowBias;
			float shadowNormalBias;
			float shadowRadius;
			vec2 shadowMapSize;
			float shadowCameraNear;
			float shadowCameraFar;
		};
		uniform PointLightShadow pointLightShadows[ NUM_POINT_LIGHT_SHADOWS ];
	#endif
#endif`,_w=`#if ( defined( USE_SHADOWMAP ) && ( NUM_DIR_LIGHT_SHADOWS > 0 || NUM_POINT_LIGHT_SHADOWS > 0 ) ) || ( NUM_SPOT_LIGHT_COORDS > 0 )
	#ifdef HAS_NORMAL
		vec3 shadowWorldNormal = inverseTransformDirection( transformedNormal, viewMatrix );
	#else
		vec3 shadowWorldNormal = vec3( 0.0 );
	#endif
	vec4 shadowWorldPosition;
#endif
#if defined( USE_SHADOWMAP )
	#if NUM_DIR_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * directionalLightShadows[ i ].shadowNormalBias, 0 );
			vDirectionalShadowCoord[ i ] = directionalShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0
		#pragma unroll_loop_start
		for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
			shadowWorldPosition = worldPosition + vec4( shadowWorldNormal * pointLightShadows[ i ].shadowNormalBias, 0 );
			vPointShadowCoord[ i ] = pointShadowMatrix[ i ] * shadowWorldPosition;
		}
		#pragma unroll_loop_end
	#endif
#endif
#if NUM_SPOT_LIGHT_COORDS > 0
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_COORDS; i ++ ) {
		shadowWorldPosition = worldPosition;
		#if ( defined( USE_SHADOWMAP ) && UNROLLED_LOOP_INDEX < NUM_SPOT_LIGHT_SHADOWS )
			shadowWorldPosition.xyz += shadowWorldNormal * spotLightShadows[ i ].shadowNormalBias;
		#endif
		vSpotLightCoord[ i ] = spotLightMatrix[ i ] * shadowWorldPosition;
	}
	#pragma unroll_loop_end
#endif`,xw=`float getShadowMask() {
	float shadow = 1.0;
	#ifdef USE_SHADOWMAP
	#if NUM_DIR_LIGHT_SHADOWS > 0
	DirectionalLightShadow directionalLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_DIR_LIGHT_SHADOWS; i ++ ) {
		directionalLight = directionalLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( directionalShadowMap[ i ], directionalLight.shadowMapSize, directionalLight.shadowIntensity, directionalLight.shadowBias, directionalLight.shadowRadius, vDirectionalShadowCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_SPOT_LIGHT_SHADOWS > 0
	SpotLightShadow spotLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_SPOT_LIGHT_SHADOWS; i ++ ) {
		spotLight = spotLightShadows[ i ];
		shadow *= receiveShadow ? getShadow( spotShadowMap[ i ], spotLight.shadowMapSize, spotLight.shadowIntensity, spotLight.shadowBias, spotLight.shadowRadius, vSpotLightCoord[ i ] ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#if NUM_POINT_LIGHT_SHADOWS > 0 && ( defined( SHADOWMAP_TYPE_PCF ) || defined( SHADOWMAP_TYPE_BASIC ) )
	PointLightShadow pointLight;
	#pragma unroll_loop_start
	for ( int i = 0; i < NUM_POINT_LIGHT_SHADOWS; i ++ ) {
		pointLight = pointLightShadows[ i ];
		shadow *= receiveShadow ? getPointShadow( pointShadowMap[ i ], pointLight.shadowMapSize, pointLight.shadowIntensity, pointLight.shadowBias, pointLight.shadowRadius, vPointShadowCoord[ i ], pointLight.shadowCameraNear, pointLight.shadowCameraFar ) : 1.0;
	}
	#pragma unroll_loop_end
	#endif
	#endif
	return shadow;
}`,Mw=`#ifdef USE_SKINNING
	mat4 boneMatX = getBoneMatrix( skinIndex.x );
	mat4 boneMatY = getBoneMatrix( skinIndex.y );
	mat4 boneMatZ = getBoneMatrix( skinIndex.z );
	mat4 boneMatW = getBoneMatrix( skinIndex.w );
#endif`,Sw=`#ifdef USE_SKINNING
	uniform mat4 bindMatrix;
	uniform mat4 bindMatrixInverse;
	uniform highp sampler2D boneTexture;
	mat4 getBoneMatrix( const in float i ) {
		int size = textureSize( boneTexture, 0 ).x;
		int j = int( i ) * 4;
		int x = j % size;
		int y = j / size;
		vec4 v1 = texelFetch( boneTexture, ivec2( x, y ), 0 );
		vec4 v2 = texelFetch( boneTexture, ivec2( x + 1, y ), 0 );
		vec4 v3 = texelFetch( boneTexture, ivec2( x + 2, y ), 0 );
		vec4 v4 = texelFetch( boneTexture, ivec2( x + 3, y ), 0 );
		return mat4( v1, v2, v3, v4 );
	}
#endif`,Ew=`#ifdef USE_SKINNING
	vec4 skinVertex = bindMatrix * vec4( transformed, 1.0 );
	vec4 skinned = vec4( 0.0 );
	skinned += boneMatX * skinVertex * skinWeight.x;
	skinned += boneMatY * skinVertex * skinWeight.y;
	skinned += boneMatZ * skinVertex * skinWeight.z;
	skinned += boneMatW * skinVertex * skinWeight.w;
	transformed = ( bindMatrixInverse * skinned ).xyz;
#endif`,bw=`#ifdef USE_SKINNING
	mat4 skinMatrix = mat4( 0.0 );
	skinMatrix += skinWeight.x * boneMatX;
	skinMatrix += skinWeight.y * boneMatY;
	skinMatrix += skinWeight.z * boneMatZ;
	skinMatrix += skinWeight.w * boneMatW;
	skinMatrix = bindMatrixInverse * skinMatrix * bindMatrix;
	objectNormal = vec4( skinMatrix * vec4( objectNormal, 0.0 ) ).xyz;
	#ifdef USE_TANGENT
		objectTangent = vec4( skinMatrix * vec4( objectTangent, 0.0 ) ).xyz;
	#endif
#endif`,Tw=`float specularStrength;
#ifdef USE_SPECULARMAP
	vec4 texelSpecular = texture2D( specularMap, vSpecularMapUv );
	specularStrength = texelSpecular.r;
#else
	specularStrength = 1.0;
#endif`,ww=`#ifdef USE_SPECULARMAP
	uniform sampler2D specularMap;
#endif`,Iw=`#if defined( TONE_MAPPING )
	gl_FragColor.rgb = toneMapping( gl_FragColor.rgb );
#endif`,Cw=`#ifndef saturate
#define saturate( a ) clamp( a, 0.0, 1.0 )
#endif
uniform float toneMappingExposure;
vec3 LinearToneMapping( vec3 color ) {
	return saturate( toneMappingExposure * color );
}
vec3 ReinhardToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	return saturate( color / ( vec3( 1.0 ) + color ) );
}
vec3 CineonToneMapping( vec3 color ) {
	color *= toneMappingExposure;
	color = max( vec3( 0.0 ), color - 0.004 );
	return pow( ( color * ( 6.2 * color + 0.5 ) ) / ( color * ( 6.2 * color + 1.7 ) + 0.06 ), vec3( 2.2 ) );
}
vec3 RRTAndODTFit( vec3 v ) {
	vec3 a = v * ( v + 0.0245786 ) - 0.000090537;
	vec3 b = v * ( 0.983729 * v + 0.4329510 ) + 0.238081;
	return a / b;
}
vec3 ACESFilmicToneMapping( vec3 color ) {
	const mat3 ACESInputMat = mat3(
		vec3( 0.59719, 0.07600, 0.02840 ),		vec3( 0.35458, 0.90834, 0.13383 ),
		vec3( 0.04823, 0.01566, 0.83777 )
	);
	const mat3 ACESOutputMat = mat3(
		vec3(  1.60475, -0.10208, -0.00327 ),		vec3( -0.53108,  1.10813, -0.07276 ),
		vec3( -0.07367, -0.00605,  1.07602 )
	);
	color *= toneMappingExposure / 0.6;
	color = ACESInputMat * color;
	color = RRTAndODTFit( color );
	color = ACESOutputMat * color;
	return saturate( color );
}
const mat3 LINEAR_REC2020_TO_LINEAR_SRGB = mat3(
	vec3( 1.6605, - 0.1246, - 0.0182 ),
	vec3( - 0.5876, 1.1329, - 0.1006 ),
	vec3( - 0.0728, - 0.0083, 1.1187 )
);
const mat3 LINEAR_SRGB_TO_LINEAR_REC2020 = mat3(
	vec3( 0.6274, 0.0691, 0.0164 ),
	vec3( 0.3293, 0.9195, 0.0880 ),
	vec3( 0.0433, 0.0113, 0.8956 )
);
vec3 agxDefaultContrastApprox( vec3 x ) {
	vec3 x2 = x * x;
	vec3 x4 = x2 * x2;
	return + 15.5 * x4 * x2
		- 40.14 * x4 * x
		+ 31.96 * x4
		- 6.868 * x2 * x
		+ 0.4298 * x2
		+ 0.1191 * x
		- 0.00232;
}
vec3 AgXToneMapping( vec3 color ) {
	const mat3 AgXInsetMatrix = mat3(
		vec3( 0.856627153315983, 0.137318972929847, 0.11189821299995 ),
		vec3( 0.0951212405381588, 0.761241990602591, 0.0767994186031903 ),
		vec3( 0.0482516061458583, 0.101439036467562, 0.811302368396859 )
	);
	const mat3 AgXOutsetMatrix = mat3(
		vec3( 1.1271005818144368, - 0.1413297634984383, - 0.14132976349843826 ),
		vec3( - 0.11060664309660323, 1.157823702216272, - 0.11060664309660294 ),
		vec3( - 0.016493938717834573, - 0.016493938717834257, 1.2519364065950405 )
	);
	const float AgxMinEv = - 12.47393;	const float AgxMaxEv = 4.026069;
	color *= toneMappingExposure;
	color = LINEAR_SRGB_TO_LINEAR_REC2020 * color;
	color = AgXInsetMatrix * color;
	color = max( color, 1e-10 );	color = log2( color );
	color = ( color - AgxMinEv ) / ( AgxMaxEv - AgxMinEv );
	color = clamp( color, 0.0, 1.0 );
	color = agxDefaultContrastApprox( color );
	color = AgXOutsetMatrix * color;
	color = pow( max( vec3( 0.0 ), color ), vec3( 2.2 ) );
	color = LINEAR_REC2020_TO_LINEAR_SRGB * color;
	color = clamp( color, 0.0, 1.0 );
	return color;
}
vec3 NeutralToneMapping( vec3 color ) {
	const float StartCompression = 0.8 - 0.04;
	const float Desaturation = 0.15;
	color *= toneMappingExposure;
	float x = min( color.r, min( color.g, color.b ) );
	float offset = x < 0.08 ? x - 6.25 * x * x : 0.04;
	color -= offset;
	float peak = max( color.r, max( color.g, color.b ) );
	if ( peak < StartCompression ) return color;
	float d = 1. - StartCompression;
	float newPeak = 1. - d * d / ( peak + d - StartCompression );
	color *= newPeak / peak;
	float g = 1. - 1. / ( Desaturation * ( peak - newPeak ) + 1. );
	return mix( color, vec3( newPeak ), g );
}
vec3 CustomToneMapping( vec3 color ) { return color; }`,Aw=`#ifdef USE_TRANSMISSION
	material.transmission = transmission;
	material.transmissionAlpha = 1.0;
	material.thickness = thickness;
	material.attenuationDistance = attenuationDistance;
	material.attenuationColor = attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		material.transmission *= texture2D( transmissionMap, vTransmissionMapUv ).r;
	#endif
	#ifdef USE_THICKNESSMAP
		material.thickness *= texture2D( thicknessMap, vThicknessMapUv ).g;
	#endif
	vec3 pos = vWorldPosition;
	vec3 v = normalize( cameraPosition - pos );
	vec3 n = inverseTransformDirection( normal, viewMatrix );
	vec4 transmitted = getIBLVolumeRefraction(
		n, v, material.roughness, material.diffuseContribution, material.specularColorBlended, material.specularF90,
		pos, modelMatrix, viewMatrix, projectionMatrix, material.dispersion, material.ior, material.thickness,
		material.attenuationColor, material.attenuationDistance );
	material.transmissionAlpha = mix( material.transmissionAlpha, transmitted.a, material.transmission );
	totalDiffuse = mix( totalDiffuse, transmitted.rgb, material.transmission );
#endif`,Dw=`#ifdef USE_TRANSMISSION
	uniform float transmission;
	uniform float thickness;
	uniform float attenuationDistance;
	uniform vec3 attenuationColor;
	#ifdef USE_TRANSMISSIONMAP
		uniform sampler2D transmissionMap;
	#endif
	#ifdef USE_THICKNESSMAP
		uniform sampler2D thicknessMap;
	#endif
	uniform vec2 transmissionSamplerSize;
	uniform sampler2D transmissionSamplerMap;
	uniform mat4 modelMatrix;
	uniform mat4 projectionMatrix;
	varying vec3 vWorldPosition;
	float w0( float a ) {
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - a + 3.0 ) - 3.0 ) + 1.0 );
	}
	float w1( float a ) {
		return ( 1.0 / 6.0 ) * ( a *  a * ( 3.0 * a - 6.0 ) + 4.0 );
	}
	float w2( float a ){
		return ( 1.0 / 6.0 ) * ( a * ( a * ( - 3.0 * a + 3.0 ) + 3.0 ) + 1.0 );
	}
	float w3( float a ) {
		return ( 1.0 / 6.0 ) * ( a * a * a );
	}
	float g0( float a ) {
		return w0( a ) + w1( a );
	}
	float g1( float a ) {
		return w2( a ) + w3( a );
	}
	float h0( float a ) {
		return - 1.0 + w1( a ) / ( w0( a ) + w1( a ) );
	}
	float h1( float a ) {
		return 1.0 + w3( a ) / ( w2( a ) + w3( a ) );
	}
	vec4 bicubic( sampler2D tex, vec2 uv, vec4 texelSize, float lod ) {
		uv = uv * texelSize.zw + 0.5;
		vec2 iuv = floor( uv );
		vec2 fuv = fract( uv );
		float g0x = g0( fuv.x );
		float g1x = g1( fuv.x );
		float h0x = h0( fuv.x );
		float h1x = h1( fuv.x );
		float h0y = h0( fuv.y );
		float h1y = h1( fuv.y );
		vec2 p0 = ( vec2( iuv.x + h0x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p1 = ( vec2( iuv.x + h1x, iuv.y + h0y ) - 0.5 ) * texelSize.xy;
		vec2 p2 = ( vec2( iuv.x + h0x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		vec2 p3 = ( vec2( iuv.x + h1x, iuv.y + h1y ) - 0.5 ) * texelSize.xy;
		return g0( fuv.y ) * ( g0x * textureLod( tex, p0, lod ) + g1x * textureLod( tex, p1, lod ) ) +
			g1( fuv.y ) * ( g0x * textureLod( tex, p2, lod ) + g1x * textureLod( tex, p3, lod ) );
	}
	vec4 textureBicubic( sampler2D sampler, vec2 uv, float lod ) {
		vec2 fLodSize = vec2( textureSize( sampler, int( lod ) ) );
		vec2 cLodSize = vec2( textureSize( sampler, int( lod + 1.0 ) ) );
		vec2 fLodSizeInv = 1.0 / fLodSize;
		vec2 cLodSizeInv = 1.0 / cLodSize;
		vec4 fSample = bicubic( sampler, uv, vec4( fLodSizeInv, fLodSize ), floor( lod ) );
		vec4 cSample = bicubic( sampler, uv, vec4( cLodSizeInv, cLodSize ), ceil( lod ) );
		return mix( fSample, cSample, fract( lod ) );
	}
	vec3 getVolumeTransmissionRay( const in vec3 n, const in vec3 v, const in float thickness, const in float ior, const in mat4 modelMatrix ) {
		vec3 refractionVector = refract( - v, normalize( n ), 1.0 / ior );
		vec3 modelScale;
		modelScale.x = length( vec3( modelMatrix[ 0 ].xyz ) );
		modelScale.y = length( vec3( modelMatrix[ 1 ].xyz ) );
		modelScale.z = length( vec3( modelMatrix[ 2 ].xyz ) );
		return normalize( refractionVector ) * thickness * modelScale;
	}
	float applyIorToRoughness( const in float roughness, const in float ior ) {
		return roughness * clamp( ior * 2.0 - 2.0, 0.0, 1.0 );
	}
	vec4 getTransmissionSample( const in vec2 fragCoord, const in float roughness, const in float ior ) {
		float lod = log2( transmissionSamplerSize.x ) * applyIorToRoughness( roughness, ior );
		return textureBicubic( transmissionSamplerMap, fragCoord.xy, lod );
	}
	vec3 volumeAttenuation( const in float transmissionDistance, const in vec3 attenuationColor, const in float attenuationDistance ) {
		if ( isinf( attenuationDistance ) ) {
			return vec3( 1.0 );
		} else {
			vec3 attenuationCoefficient = -log( attenuationColor ) / attenuationDistance;
			vec3 transmittance = exp( - attenuationCoefficient * transmissionDistance );			return transmittance;
		}
	}
	vec4 getIBLVolumeRefraction( const in vec3 n, const in vec3 v, const in float roughness, const in vec3 diffuseColor,
		const in vec3 specularColor, const in float specularF90, const in vec3 position, const in mat4 modelMatrix,
		const in mat4 viewMatrix, const in mat4 projMatrix, const in float dispersion, const in float ior, const in float thickness,
		const in vec3 attenuationColor, const in float attenuationDistance ) {
		vec4 transmittedLight;
		vec3 transmittance;
		#ifdef USE_DISPERSION
			float halfSpread = ( ior - 1.0 ) * 0.025 * dispersion;
			vec3 iors = vec3( ior - halfSpread, ior, ior + halfSpread );
			for ( int i = 0; i < 3; i ++ ) {
				vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, iors[ i ], modelMatrix );
				vec3 refractedRayExit = position + transmissionRay;
				vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
				vec2 refractionCoords = ndcPos.xy / ndcPos.w;
				refractionCoords += 1.0;
				refractionCoords /= 2.0;
				vec4 transmissionSample = getTransmissionSample( refractionCoords, roughness, iors[ i ] );
				transmittedLight[ i ] = transmissionSample[ i ];
				transmittedLight.a += transmissionSample.a;
				transmittance[ i ] = diffuseColor[ i ] * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance )[ i ];
			}
			transmittedLight.a /= 3.0;
		#else
			vec3 transmissionRay = getVolumeTransmissionRay( n, v, thickness, ior, modelMatrix );
			vec3 refractedRayExit = position + transmissionRay;
			vec4 ndcPos = projMatrix * viewMatrix * vec4( refractedRayExit, 1.0 );
			vec2 refractionCoords = ndcPos.xy / ndcPos.w;
			refractionCoords += 1.0;
			refractionCoords /= 2.0;
			transmittedLight = getTransmissionSample( refractionCoords, roughness, ior );
			transmittance = diffuseColor * volumeAttenuation( length( transmissionRay ), attenuationColor, attenuationDistance );
		#endif
		vec3 attenuatedColor = transmittance * transmittedLight.rgb;
		vec3 F = EnvironmentBRDF( n, v, specularColor, specularF90, roughness );
		float transmittanceFactor = ( transmittance.r + transmittance.g + transmittance.b ) / 3.0;
		return vec4( ( 1.0 - F ) * attenuatedColor, 1.0 - ( 1.0 - transmittedLight.a ) * transmittanceFactor );
	}
#endif`,Rw=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_SPECULARMAP
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,Nw=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	varying vec2 vUv;
#endif
#ifdef USE_MAP
	uniform mat3 mapTransform;
	varying vec2 vMapUv;
#endif
#ifdef USE_ALPHAMAP
	uniform mat3 alphaMapTransform;
	varying vec2 vAlphaMapUv;
#endif
#ifdef USE_LIGHTMAP
	uniform mat3 lightMapTransform;
	varying vec2 vLightMapUv;
#endif
#ifdef USE_AOMAP
	uniform mat3 aoMapTransform;
	varying vec2 vAoMapUv;
#endif
#ifdef USE_BUMPMAP
	uniform mat3 bumpMapTransform;
	varying vec2 vBumpMapUv;
#endif
#ifdef USE_NORMALMAP
	uniform mat3 normalMapTransform;
	varying vec2 vNormalMapUv;
#endif
#ifdef USE_DISPLACEMENTMAP
	uniform mat3 displacementMapTransform;
	varying vec2 vDisplacementMapUv;
#endif
#ifdef USE_EMISSIVEMAP
	uniform mat3 emissiveMapTransform;
	varying vec2 vEmissiveMapUv;
#endif
#ifdef USE_METALNESSMAP
	uniform mat3 metalnessMapTransform;
	varying vec2 vMetalnessMapUv;
#endif
#ifdef USE_ROUGHNESSMAP
	uniform mat3 roughnessMapTransform;
	varying vec2 vRoughnessMapUv;
#endif
#ifdef USE_ANISOTROPYMAP
	uniform mat3 anisotropyMapTransform;
	varying vec2 vAnisotropyMapUv;
#endif
#ifdef USE_CLEARCOATMAP
	uniform mat3 clearcoatMapTransform;
	varying vec2 vClearcoatMapUv;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	uniform mat3 clearcoatNormalMapTransform;
	varying vec2 vClearcoatNormalMapUv;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	uniform mat3 clearcoatRoughnessMapTransform;
	varying vec2 vClearcoatRoughnessMapUv;
#endif
#ifdef USE_SHEEN_COLORMAP
	uniform mat3 sheenColorMapTransform;
	varying vec2 vSheenColorMapUv;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	uniform mat3 sheenRoughnessMapTransform;
	varying vec2 vSheenRoughnessMapUv;
#endif
#ifdef USE_IRIDESCENCEMAP
	uniform mat3 iridescenceMapTransform;
	varying vec2 vIridescenceMapUv;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	uniform mat3 iridescenceThicknessMapTransform;
	varying vec2 vIridescenceThicknessMapUv;
#endif
#ifdef USE_SPECULARMAP
	uniform mat3 specularMapTransform;
	varying vec2 vSpecularMapUv;
#endif
#ifdef USE_SPECULAR_COLORMAP
	uniform mat3 specularColorMapTransform;
	varying vec2 vSpecularColorMapUv;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	uniform mat3 specularIntensityMapTransform;
	varying vec2 vSpecularIntensityMapUv;
#endif
#ifdef USE_TRANSMISSIONMAP
	uniform mat3 transmissionMapTransform;
	varying vec2 vTransmissionMapUv;
#endif
#ifdef USE_THICKNESSMAP
	uniform mat3 thicknessMapTransform;
	varying vec2 vThicknessMapUv;
#endif`,Pw=`#if defined( USE_UV ) || defined( USE_ANISOTROPY )
	vUv = vec3( uv, 1 ).xy;
#endif
#ifdef USE_MAP
	vMapUv = ( mapTransform * vec3( MAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ALPHAMAP
	vAlphaMapUv = ( alphaMapTransform * vec3( ALPHAMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_LIGHTMAP
	vLightMapUv = ( lightMapTransform * vec3( LIGHTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_AOMAP
	vAoMapUv = ( aoMapTransform * vec3( AOMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_BUMPMAP
	vBumpMapUv = ( bumpMapTransform * vec3( BUMPMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_NORMALMAP
	vNormalMapUv = ( normalMapTransform * vec3( NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_DISPLACEMENTMAP
	vDisplacementMapUv = ( displacementMapTransform * vec3( DISPLACEMENTMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_EMISSIVEMAP
	vEmissiveMapUv = ( emissiveMapTransform * vec3( EMISSIVEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_METALNESSMAP
	vMetalnessMapUv = ( metalnessMapTransform * vec3( METALNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ROUGHNESSMAP
	vRoughnessMapUv = ( roughnessMapTransform * vec3( ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_ANISOTROPYMAP
	vAnisotropyMapUv = ( anisotropyMapTransform * vec3( ANISOTROPYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOATMAP
	vClearcoatMapUv = ( clearcoatMapTransform * vec3( CLEARCOATMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_NORMALMAP
	vClearcoatNormalMapUv = ( clearcoatNormalMapTransform * vec3( CLEARCOAT_NORMALMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_CLEARCOAT_ROUGHNESSMAP
	vClearcoatRoughnessMapUv = ( clearcoatRoughnessMapTransform * vec3( CLEARCOAT_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCEMAP
	vIridescenceMapUv = ( iridescenceMapTransform * vec3( IRIDESCENCEMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_IRIDESCENCE_THICKNESSMAP
	vIridescenceThicknessMapUv = ( iridescenceThicknessMapTransform * vec3( IRIDESCENCE_THICKNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_COLORMAP
	vSheenColorMapUv = ( sheenColorMapTransform * vec3( SHEEN_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SHEEN_ROUGHNESSMAP
	vSheenRoughnessMapUv = ( sheenRoughnessMapTransform * vec3( SHEEN_ROUGHNESSMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULARMAP
	vSpecularMapUv = ( specularMapTransform * vec3( SPECULARMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_COLORMAP
	vSpecularColorMapUv = ( specularColorMapTransform * vec3( SPECULAR_COLORMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_SPECULAR_INTENSITYMAP
	vSpecularIntensityMapUv = ( specularIntensityMapTransform * vec3( SPECULAR_INTENSITYMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_TRANSMISSIONMAP
	vTransmissionMapUv = ( transmissionMapTransform * vec3( TRANSMISSIONMAP_UV, 1 ) ).xy;
#endif
#ifdef USE_THICKNESSMAP
	vThicknessMapUv = ( thicknessMapTransform * vec3( THICKNESSMAP_UV, 1 ) ).xy;
#endif`,Lw=`#if defined( USE_ENVMAP ) || defined( DISTANCE ) || defined ( USE_SHADOWMAP ) || defined ( USE_TRANSMISSION ) || NUM_SPOT_LIGHT_COORDS > 0
	vec4 worldPosition = vec4( transformed, 1.0 );
	#ifdef USE_BATCHING
		worldPosition = batchingMatrix * worldPosition;
	#endif
	#ifdef USE_INSTANCING
		worldPosition = instanceMatrix * worldPosition;
	#endif
	worldPosition = modelMatrix * worldPosition;
#endif`,Ow=`varying vec2 vUv;
uniform mat3 uvTransform;
void main() {
	vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	gl_Position = vec4( position.xy, 1.0, 1.0 );
}`,Fw=`uniform sampler2D t2D;
uniform float backgroundIntensity;
varying vec2 vUv;
void main() {
	vec4 texColor = texture2D( t2D, vUv );
	#ifdef DECODE_VIDEO_TEXTURE
		texColor = vec4( mix( pow( texColor.rgb * 0.9478672986 + vec3( 0.0521327014 ), vec3( 2.4 ) ), texColor.rgb * 0.0773993808, vec3( lessThanEqual( texColor.rgb, vec3( 0.04045 ) ) ) ), texColor.w );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,kw=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,Uw=`#ifdef ENVMAP_TYPE_CUBE
	uniform samplerCube envMap;
#elif defined( ENVMAP_TYPE_CUBE_UV )
	uniform sampler2D envMap;
#endif
uniform float backgroundBlurriness;
uniform float backgroundIntensity;
uniform mat3 backgroundRotation;
varying vec3 vWorldDirection;
#include <cube_uv_reflection_fragment>
void main() {
	#ifdef ENVMAP_TYPE_CUBE
		vec4 texColor = textureCube( envMap, backgroundRotation * vWorldDirection );
	#elif defined( ENVMAP_TYPE_CUBE_UV )
		vec4 texColor = textureCubeUV( envMap, backgroundRotation * vWorldDirection, backgroundBlurriness );
	#else
		vec4 texColor = vec4( 0.0, 0.0, 0.0, 1.0 );
	#endif
	texColor.rgb *= backgroundIntensity;
	gl_FragColor = texColor;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,Bw=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
	gl_Position.z = gl_Position.w;
}`,Vw=`uniform samplerCube tCube;
uniform float tFlip;
uniform float opacity;
varying vec3 vWorldDirection;
void main() {
	vec4 texColor = textureCube( tCube, vec3( tFlip * vWorldDirection.x, vWorldDirection.yz ) );
	gl_FragColor = texColor;
	gl_FragColor.a *= opacity;
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,Hw=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
varying vec2 vHighPrecisionZW;
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vHighPrecisionZW = gl_Position.zw;
}`,zw=`#if DEPTH_PACKING == 3200
	uniform float opacity;
#endif
#include <common>
#include <packing>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
varying vec2 vHighPrecisionZW;
void main() {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#if DEPTH_PACKING == 3200
		diffuseColor.a = opacity;
	#endif
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <logdepthbuf_fragment>
	#ifdef USE_REVERSED_DEPTH_BUFFER
		float fragCoordZ = vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ];
	#else
		float fragCoordZ = 0.5 * vHighPrecisionZW[ 0 ] / vHighPrecisionZW[ 1 ] + 0.5;
	#endif
	#if DEPTH_PACKING == 3200
		gl_FragColor = vec4( vec3( 1.0 - fragCoordZ ), opacity );
	#elif DEPTH_PACKING == 3201
		gl_FragColor = packDepthToRGBA( fragCoordZ );
	#elif DEPTH_PACKING == 3202
		gl_FragColor = vec4( packDepthToRGB( fragCoordZ ), 1.0 );
	#elif DEPTH_PACKING == 3203
		gl_FragColor = vec4( packDepthToRG( fragCoordZ ), 0.0, 1.0 );
	#endif
}`,Gw=`#define DISTANCE
varying vec3 vWorldPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <skinbase_vertex>
	#include <morphinstance_vertex>
	#ifdef USE_DISPLACEMENTMAP
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <worldpos_vertex>
	#include <clipping_planes_vertex>
	vWorldPosition = worldPosition.xyz;
}`,Ww=`#define DISTANCE
uniform vec3 referencePosition;
uniform float nearDistance;
uniform float farDistance;
varying vec3 vWorldPosition;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <clipping_planes_pars_fragment>
void main () {
	vec4 diffuseColor = vec4( 1.0 );
	#include <clipping_planes_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	float dist = length( vWorldPosition - referencePosition );
	dist = ( dist - nearDistance ) / ( farDistance - nearDistance );
	dist = saturate( dist );
	gl_FragColor = vec4( dist, 0.0, 0.0, 1.0 );
}`,jw=`varying vec3 vWorldDirection;
#include <common>
void main() {
	vWorldDirection = transformDirection( position, modelMatrix );
	#include <begin_vertex>
	#include <project_vertex>
}`,$w=`uniform sampler2D tEquirect;
varying vec3 vWorldDirection;
#include <common>
void main() {
	vec3 direction = normalize( vWorldDirection );
	vec2 sampleUV = equirectUv( direction );
	gl_FragColor = texture2D( tEquirect, sampleUV );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
}`,qw=`uniform float scale;
attribute float lineDistance;
varying float vLineDistance;
#include <common>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	vLineDistance = scale * lineDistance;
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,Xw=`uniform vec3 diffuse;
uniform float opacity;
uniform float dashSize;
uniform float totalSize;
varying float vLineDistance;
#include <common>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	if ( mod( vLineDistance, totalSize ) > dashSize ) {
		discard;
	}
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,Yw=`#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#if defined ( USE_ENVMAP ) || defined ( USE_SKINNING )
		#include <beginnormal_vertex>
		#include <morphnormal_vertex>
		#include <skinbase_vertex>
		#include <skinnormal_vertex>
		#include <defaultnormal_vertex>
	#endif
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <fog_vertex>
}`,Zw=`uniform vec3 diffuse;
uniform float opacity;
#ifndef FLAT_SHADED
	varying vec3 vNormal;
#endif
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <fog_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	#ifdef USE_LIGHTMAP
		vec4 lightMapTexel = texture2D( lightMap, vLightMapUv );
		reflectedLight.indirectDiffuse += lightMapTexel.rgb * lightMapIntensity * RECIPROCAL_PI;
	#else
		reflectedLight.indirectDiffuse += vec3( 1.0 );
	#endif
	#include <aomap_fragment>
	reflectedLight.indirectDiffuse *= diffuseColor.rgb;
	vec3 outgoingLight = reflectedLight.indirectDiffuse;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,Jw=`#define LAMBERT
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,Kw=`#define LAMBERT
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_lambert_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_lambert_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,Qw=`#define MATCAP
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <color_pars_vertex>
#include <displacementmap_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
	vViewPosition = - mvPosition.xyz;
}`,eI=`#define MATCAP
uniform vec3 diffuse;
uniform float opacity;
uniform sampler2D matcap;
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	vec3 viewDir = normalize( vViewPosition );
	vec3 x = normalize( vec3( viewDir.z, 0.0, - viewDir.x ) );
	vec3 y = cross( viewDir, x );
	vec2 uv = vec2( dot( x, normal ), dot( y, normal ) ) * 0.495 + 0.5;
	#ifdef USE_MATCAP
		vec4 matcapColor = texture2D( matcap, uv );
	#else
		vec4 matcapColor = vec4( vec3( mix( 0.2, 0.8, uv.y ) ), 1.0 );
	#endif
	vec3 outgoingLight = diffuseColor.rgb * matcapColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,tI=`#define NORMAL
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	vViewPosition = - mvPosition.xyz;
#endif
}`,nI=`#define NORMAL
uniform float opacity;
#if defined( FLAT_SHADED ) || defined( USE_BUMPMAP ) || defined( USE_NORMALMAP_TANGENTSPACE )
	varying vec3 vViewPosition;
#endif
#include <uv_pars_fragment>
#include <normal_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( 0.0, 0.0, 0.0, opacity );
	#include <clipping_planes_fragment>
	#include <logdepthbuf_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	gl_FragColor = vec4( normalize( normal ) * 0.5 + 0.5, diffuseColor.a );
	#ifdef OPAQUE
		gl_FragColor.a = 1.0;
	#endif
}`,iI=`#define PHONG
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <envmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <envmap_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,rI=`#define PHONG
uniform vec3 diffuse;
uniform vec3 emissive;
uniform vec3 specular;
uniform float shininess;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_phong_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <specularmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <specularmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_phong_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + reflectedLight.directSpecular + reflectedLight.indirectSpecular + totalEmissiveRadiance;
	#include <envmap_fragment>
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,sI=`#define STANDARD
varying vec3 vViewPosition;
#ifdef USE_TRANSMISSION
	varying vec3 vWorldPosition;
#endif
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
#ifdef USE_TRANSMISSION
	vWorldPosition = worldPosition.xyz;
#endif
}`,oI=`#define STANDARD
#ifdef PHYSICAL
	#define IOR
	#define USE_SPECULAR
#endif
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float roughness;
uniform float metalness;
uniform float opacity;
#ifdef IOR
	uniform float ior;
#endif
#ifdef USE_SPECULAR
	uniform float specularIntensity;
	uniform vec3 specularColor;
	#ifdef USE_SPECULAR_COLORMAP
		uniform sampler2D specularColorMap;
	#endif
	#ifdef USE_SPECULAR_INTENSITYMAP
		uniform sampler2D specularIntensityMap;
	#endif
#endif
#ifdef USE_CLEARCOAT
	uniform float clearcoat;
	uniform float clearcoatRoughness;
#endif
#ifdef USE_DISPERSION
	uniform float dispersion;
#endif
#ifdef USE_IRIDESCENCE
	uniform float iridescence;
	uniform float iridescenceIOR;
	uniform float iridescenceThicknessMinimum;
	uniform float iridescenceThicknessMaximum;
#endif
#ifdef USE_SHEEN
	uniform vec3 sheenColor;
	uniform float sheenRoughness;
	#ifdef USE_SHEEN_COLORMAP
		uniform sampler2D sheenColorMap;
	#endif
	#ifdef USE_SHEEN_ROUGHNESSMAP
		uniform sampler2D sheenRoughnessMap;
	#endif
#endif
#ifdef USE_ANISOTROPY
	uniform vec2 anisotropyVector;
	#ifdef USE_ANISOTROPYMAP
		uniform sampler2D anisotropyMap;
	#endif
#endif
varying vec3 vViewPosition;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <iridescence_fragment>
#include <cube_uv_reflection_fragment>
#include <envmap_common_pars_fragment>
#include <envmap_physical_pars_fragment>
#include <fog_pars_fragment>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_physical_pars_fragment>
#include <transmission_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <clearcoat_pars_fragment>
#include <iridescence_pars_fragment>
#include <roughnessmap_pars_fragment>
#include <metalnessmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <roughnessmap_fragment>
	#include <metalnessmap_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <clearcoat_normal_fragment_begin>
	#include <clearcoat_normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_physical_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 totalDiffuse = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse;
	vec3 totalSpecular = reflectedLight.directSpecular + reflectedLight.indirectSpecular;
	#include <transmission_fragment>
	vec3 outgoingLight = totalDiffuse + totalSpecular + totalEmissiveRadiance;
	#ifdef USE_SHEEN
 
		outgoingLight = outgoingLight + sheenSpecularDirect + sheenSpecularIndirect;
 
 	#endif
	#ifdef USE_CLEARCOAT
		float dotNVcc = saturate( dot( geometryClearcoatNormal, geometryViewDir ) );
		vec3 Fcc = F_Schlick( material.clearcoatF0, material.clearcoatF90, dotNVcc );
		outgoingLight = outgoingLight * ( 1.0 - material.clearcoat * Fcc ) + ( clearcoatSpecularDirect + clearcoatSpecularIndirect ) * material.clearcoat;
	#endif
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,aI=`#define TOON
varying vec3 vViewPosition;
#include <common>
#include <batching_pars_vertex>
#include <uv_pars_vertex>
#include <displacementmap_pars_vertex>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <normal_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <shadowmap_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <normal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <displacementmap_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	vViewPosition = - mvPosition.xyz;
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,cI=`#define TOON
uniform vec3 diffuse;
uniform vec3 emissive;
uniform float opacity;
#include <common>
#include <dithering_pars_fragment>
#include <color_pars_fragment>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <aomap_pars_fragment>
#include <lightmap_pars_fragment>
#include <emissivemap_pars_fragment>
#include <gradientmap_pars_fragment>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <normal_pars_fragment>
#include <lights_toon_pars_fragment>
#include <shadowmap_pars_fragment>
#include <bumpmap_pars_fragment>
#include <normalmap_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	ReflectedLight reflectedLight = ReflectedLight( vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ), vec3( 0.0 ) );
	vec3 totalEmissiveRadiance = emissive;
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <color_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	#include <normal_fragment_begin>
	#include <normal_fragment_maps>
	#include <emissivemap_fragment>
	#include <lights_toon_fragment>
	#include <lights_fragment_begin>
	#include <lights_fragment_maps>
	#include <lights_fragment_end>
	#include <aomap_fragment>
	vec3 outgoingLight = reflectedLight.directDiffuse + reflectedLight.indirectDiffuse + totalEmissiveRadiance;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
	#include <dithering_fragment>
}`,lI=`uniform float size;
uniform float scale;
#include <common>
#include <color_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
#ifdef USE_POINTS_UV
	varying vec2 vUv;
	uniform mat3 uvTransform;
#endif
void main() {
	#ifdef USE_POINTS_UV
		vUv = ( uvTransform * vec3( uv, 1 ) ).xy;
	#endif
	#include <color_vertex>
	#include <morphinstance_vertex>
	#include <morphcolor_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <project_vertex>
	gl_PointSize = size;
	#ifdef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) gl_PointSize *= ( scale / - mvPosition.z );
	#endif
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <worldpos_vertex>
	#include <fog_vertex>
}`,uI=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <color_pars_fragment>
#include <map_particle_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_particle_fragment>
	#include <color_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,dI=`#include <common>
#include <batching_pars_vertex>
#include <fog_pars_vertex>
#include <morphtarget_pars_vertex>
#include <skinning_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <shadowmap_pars_vertex>
void main() {
	#include <batching_vertex>
	#include <beginnormal_vertex>
	#include <morphinstance_vertex>
	#include <morphnormal_vertex>
	#include <skinbase_vertex>
	#include <skinnormal_vertex>
	#include <defaultnormal_vertex>
	#include <begin_vertex>
	#include <morphtarget_vertex>
	#include <skinning_vertex>
	#include <project_vertex>
	#include <logdepthbuf_vertex>
	#include <worldpos_vertex>
	#include <shadowmap_vertex>
	#include <fog_vertex>
}`,fI=`uniform vec3 color;
uniform float opacity;
#include <common>
#include <fog_pars_fragment>
#include <bsdfs>
#include <lights_pars_begin>
#include <logdepthbuf_pars_fragment>
#include <shadowmap_pars_fragment>
#include <shadowmask_pars_fragment>
void main() {
	#include <logdepthbuf_fragment>
	gl_FragColor = vec4( color, opacity * ( 1.0 - getShadowMask() ) );
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
	#include <premultiplied_alpha_fragment>
}`,hI=`uniform float rotation;
uniform vec2 center;
#include <common>
#include <uv_pars_vertex>
#include <fog_pars_vertex>
#include <logdepthbuf_pars_vertex>
#include <clipping_planes_pars_vertex>
void main() {
	#include <uv_vertex>
	vec4 mvPosition = modelViewMatrix[ 3 ];
	vec2 scale = vec2( length( modelMatrix[ 0 ].xyz ), length( modelMatrix[ 1 ].xyz ) );
	#ifndef USE_SIZEATTENUATION
		bool isPerspective = isPerspectiveMatrix( projectionMatrix );
		if ( isPerspective ) scale *= - mvPosition.z;
	#endif
	vec2 alignedPosition = ( position.xy - ( center - vec2( 0.5 ) ) ) * scale;
	vec2 rotatedPosition;
	rotatedPosition.x = cos( rotation ) * alignedPosition.x - sin( rotation ) * alignedPosition.y;
	rotatedPosition.y = sin( rotation ) * alignedPosition.x + cos( rotation ) * alignedPosition.y;
	mvPosition.xy += rotatedPosition;
	gl_Position = projectionMatrix * mvPosition;
	#include <logdepthbuf_vertex>
	#include <clipping_planes_vertex>
	#include <fog_vertex>
}`,pI=`uniform vec3 diffuse;
uniform float opacity;
#include <common>
#include <uv_pars_fragment>
#include <map_pars_fragment>
#include <alphamap_pars_fragment>
#include <alphatest_pars_fragment>
#include <alphahash_pars_fragment>
#include <fog_pars_fragment>
#include <logdepthbuf_pars_fragment>
#include <clipping_planes_pars_fragment>
void main() {
	vec4 diffuseColor = vec4( diffuse, opacity );
	#include <clipping_planes_fragment>
	vec3 outgoingLight = vec3( 0.0 );
	#include <logdepthbuf_fragment>
	#include <map_fragment>
	#include <alphamap_fragment>
	#include <alphatest_fragment>
	#include <alphahash_fragment>
	outgoingLight = diffuseColor.rgb;
	#include <opaque_fragment>
	#include <tonemapping_fragment>
	#include <colorspace_fragment>
	#include <fog_fragment>
}`,ke={alphahash_fragment:Ob,alphahash_pars_fragment:Fb,alphamap_fragment:kb,alphamap_pars_fragment:Ub,alphatest_fragment:Bb,alphatest_pars_fragment:Vb,aomap_fragment:Hb,aomap_pars_fragment:zb,batching_pars_vertex:Gb,batching_vertex:Wb,begin_vertex:jb,beginnormal_vertex:$b,bsdfs:qb,iridescence_fragment:Xb,bumpmap_pars_fragment:Yb,clipping_planes_fragment:Zb,clipping_planes_pars_fragment:Jb,clipping_planes_pars_vertex:Kb,clipping_planes_vertex:Qb,color_fragment:eT,color_pars_fragment:tT,color_pars_vertex:nT,color_vertex:iT,common:rT,cube_uv_reflection_fragment:sT,defaultnormal_vertex:oT,displacementmap_pars_vertex:aT,displacementmap_vertex:cT,emissivemap_fragment:lT,emissivemap_pars_fragment:uT,colorspace_fragment:dT,colorspace_pars_fragment:fT,envmap_fragment:hT,envmap_common_pars_fragment:pT,envmap_pars_fragment:mT,envmap_pars_vertex:gT,envmap_physical_pars_fragment:IT,envmap_vertex:vT,fog_vertex:yT,fog_pars_vertex:_T,fog_fragment:xT,fog_pars_fragment:MT,gradientmap_pars_fragment:ST,lightmap_pars_fragment:ET,lights_lambert_fragment:bT,lights_lambert_pars_fragment:TT,lights_pars_begin:wT,lights_toon_fragment:CT,lights_toon_pars_fragment:AT,lights_phong_fragment:DT,lights_phong_pars_fragment:RT,lights_physical_fragment:NT,lights_physical_pars_fragment:PT,lights_fragment_begin:LT,lights_fragment_maps:OT,lights_fragment_end:FT,lightprobes_pars_fragment:kT,logdepthbuf_fragment:UT,logdepthbuf_pars_fragment:BT,logdepthbuf_pars_vertex:VT,logdepthbuf_vertex:HT,map_fragment:zT,map_pars_fragment:GT,map_particle_fragment:WT,map_particle_pars_fragment:jT,metalnessmap_fragment:$T,metalnessmap_pars_fragment:qT,morphinstance_vertex:XT,morphcolor_vertex:YT,morphnormal_vertex:ZT,morphtarget_pars_vertex:JT,morphtarget_vertex:KT,normal_fragment_begin:QT,normal_fragment_maps:ew,normal_pars_fragment:tw,normal_pars_vertex:nw,normal_vertex:iw,normalmap_pars_fragment:rw,clearcoat_normal_fragment_begin:sw,clearcoat_normal_fragment_maps:ow,clearcoat_pars_fragment:aw,iridescence_pars_fragment:cw,opaque_fragment:lw,packing:uw,premultiplied_alpha_fragment:dw,project_vertex:fw,dithering_fragment:hw,dithering_pars_fragment:pw,roughnessmap_fragment:mw,roughnessmap_pars_fragment:gw,shadowmap_pars_fragment:vw,shadowmap_pars_vertex:yw,shadowmap_vertex:_w,shadowmask_pars_fragment:xw,skinbase_vertex:Mw,skinning_pars_vertex:Sw,skinning_vertex:Ew,skinnormal_vertex:bw,specularmap_fragment:Tw,specularmap_pars_fragment:ww,tonemapping_fragment:Iw,tonemapping_pars_fragment:Cw,transmission_fragment:Aw,transmission_pars_fragment:Dw,uv_pars_fragment:Rw,uv_pars_vertex:Nw,uv_vertex:Pw,worldpos_vertex:Lw,background_vert:Ow,background_frag:Fw,backgroundCube_vert:kw,backgroundCube_frag:Uw,cube_vert:Bw,cube_frag:Vw,depth_vert:Hw,depth_frag:zw,distance_vert:Gw,distance_frag:Ww,equirect_vert:jw,equirect_frag:$w,linedashed_vert:qw,linedashed_frag:Xw,meshbasic_vert:Yw,meshbasic_frag:Zw,meshlambert_vert:Jw,meshlambert_frag:Kw,meshmatcap_vert:Qw,meshmatcap_frag:eI,meshnormal_vert:tI,meshnormal_frag:nI,meshphong_vert:iI,meshphong_frag:rI,meshphysical_vert:sI,meshphysical_frag:oI,meshtoon_vert:aI,meshtoon_frag:cI,points_vert:lI,points_frag:uI,shadow_vert:dI,shadow_frag:fI,sprite_vert:hI,sprite_frag:pI},ce={common:{diffuse:{value:new Ze(16777215)},opacity:{value:1},map:{value:null},mapTransform:{value:new Ae},alphaMap:{value:null},alphaMapTransform:{value:new Ae},alphaTest:{value:0}},specularmap:{specularMap:{value:null},specularMapTransform:{value:new Ae}},envmap:{envMap:{value:null},envMapRotation:{value:new Ae},reflectivity:{value:1},ior:{value:1.5},refractionRatio:{value:.98},dfgLUT:{value:null}},aomap:{aoMap:{value:null},aoMapIntensity:{value:1},aoMapTransform:{value:new Ae}},lightmap:{lightMap:{value:null},lightMapIntensity:{value:1},lightMapTransform:{value:new Ae}},bumpmap:{bumpMap:{value:null},bumpMapTransform:{value:new Ae},bumpScale:{value:1}},normalmap:{normalMap:{value:null},normalMapTransform:{value:new Ae},normalScale:{value:new tt(1,1)}},displacementmap:{displacementMap:{value:null},displacementMapTransform:{value:new Ae},displacementScale:{value:1},displacementBias:{value:0}},emissivemap:{emissiveMap:{value:null},emissiveMapTransform:{value:new Ae}},metalnessmap:{metalnessMap:{value:null},metalnessMapTransform:{value:new Ae}},roughnessmap:{roughnessMap:{value:null},roughnessMapTransform:{value:new Ae}},gradientmap:{gradientMap:{value:null}},fog:{fogDensity:{value:25e-5},fogNear:{value:1},fogFar:{value:2e3},fogColor:{value:new Ze(16777215)}},lights:{ambientLightColor:{value:[]},lightProbe:{value:[]},directionalLights:{value:[],properties:{direction:{},color:{}}},directionalLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},directionalShadowMatrix:{value:[]},spotLights:{value:[],properties:{color:{},position:{},direction:{},distance:{},coneCos:{},penumbraCos:{},decay:{}}},spotLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{}}},spotLightMap:{value:[]},spotLightMatrix:{value:[]},pointLights:{value:[],properties:{color:{},position:{},decay:{},distance:{}}},pointLightShadows:{value:[],properties:{shadowIntensity:1,shadowBias:{},shadowNormalBias:{},shadowRadius:{},shadowMapSize:{},shadowCameraNear:{},shadowCameraFar:{}}},pointShadowMatrix:{value:[]},hemisphereLights:{value:[],properties:{direction:{},skyColor:{},groundColor:{}}},rectAreaLights:{value:[],properties:{color:{},position:{},width:{},height:{}}},ltc_1:{value:null},ltc_2:{value:null},probesSH:{value:null},probesMin:{value:new V},probesMax:{value:new V},probesResolution:{value:new V}},points:{diffuse:{value:new Ze(16777215)},opacity:{value:1},size:{value:1},scale:{value:1},map:{value:null},alphaMap:{value:null},alphaMapTransform:{value:new Ae},alphaTest:{value:0},uvTransform:{value:new Ae}},sprite:{diffuse:{value:new Ze(16777215)},opacity:{value:1},center:{value:new tt(.5,.5)},rotation:{value:0},map:{value:null},mapTransform:{value:new Ae},alphaMap:{value:null},alphaMapTransform:{value:new Ae},alphaTest:{value:0}}},ni={basic:{uniforms:zt([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.fog]),vertexShader:ke.meshbasic_vert,fragmentShader:ke.meshbasic_frag},lambert:{uniforms:zt([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new Ze(0)},envMapIntensity:{value:1}}]),vertexShader:ke.meshlambert_vert,fragmentShader:ke.meshlambert_frag},phong:{uniforms:zt([ce.common,ce.specularmap,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,ce.lights,{emissive:{value:new Ze(0)},specular:{value:new Ze(1118481)},shininess:{value:30},envMapIntensity:{value:1}}]),vertexShader:ke.meshphong_vert,fragmentShader:ke.meshphong_frag},standard:{uniforms:zt([ce.common,ce.envmap,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.roughnessmap,ce.metalnessmap,ce.fog,ce.lights,{emissive:{value:new Ze(0)},roughness:{value:1},metalness:{value:0},envMapIntensity:{value:1}}]),vertexShader:ke.meshphysical_vert,fragmentShader:ke.meshphysical_frag},toon:{uniforms:zt([ce.common,ce.aomap,ce.lightmap,ce.emissivemap,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.gradientmap,ce.fog,ce.lights,{emissive:{value:new Ze(0)}}]),vertexShader:ke.meshtoon_vert,fragmentShader:ke.meshtoon_frag},matcap:{uniforms:zt([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,ce.fog,{matcap:{value:null}}]),vertexShader:ke.meshmatcap_vert,fragmentShader:ke.meshmatcap_frag},points:{uniforms:zt([ce.points,ce.fog]),vertexShader:ke.points_vert,fragmentShader:ke.points_frag},dashed:{uniforms:zt([ce.common,ce.fog,{scale:{value:1},dashSize:{value:1},totalSize:{value:2}}]),vertexShader:ke.linedashed_vert,fragmentShader:ke.linedashed_frag},depth:{uniforms:zt([ce.common,ce.displacementmap]),vertexShader:ke.depth_vert,fragmentShader:ke.depth_frag},normal:{uniforms:zt([ce.common,ce.bumpmap,ce.normalmap,ce.displacementmap,{opacity:{value:1}}]),vertexShader:ke.meshnormal_vert,fragmentShader:ke.meshnormal_frag},sprite:{uniforms:zt([ce.sprite,ce.fog]),vertexShader:ke.sprite_vert,fragmentShader:ke.sprite_frag},background:{uniforms:{uvTransform:{value:new Ae},t2D:{value:null},backgroundIntensity:{value:1}},vertexShader:ke.background_vert,fragmentShader:ke.background_frag},backgroundCube:{uniforms:{envMap:{value:null},backgroundBlurriness:{value:0},backgroundIntensity:{value:1},backgroundRotation:{value:new Ae}},vertexShader:ke.backgroundCube_vert,fragmentShader:ke.backgroundCube_frag},cube:{uniforms:{tCube:{value:null},tFlip:{value:-1},opacity:{value:1}},vertexShader:ke.cube_vert,fragmentShader:ke.cube_frag},equirect:{uniforms:{tEquirect:{value:null}},vertexShader:ke.equirect_vert,fragmentShader:ke.equirect_frag},distance:{uniforms:zt([ce.common,ce.displacementmap,{referencePosition:{value:new V},nearDistance:{value:1},farDistance:{value:1e3}}]),vertexShader:ke.distance_vert,fragmentShader:ke.distance_frag},shadow:{uniforms:zt([ce.lights,ce.fog,{color:{value:new Ze(0)},opacity:{value:1}}]),vertexShader:ke.shadow_vert,fragmentShader:ke.shadow_frag}};ni.physical={uniforms:zt([ni.standard.uniforms,{clearcoat:{value:0},clearcoatMap:{value:null},clearcoatMapTransform:{value:new Ae},clearcoatNormalMap:{value:null},clearcoatNormalMapTransform:{value:new Ae},clearcoatNormalScale:{value:new tt(1,1)},clearcoatRoughness:{value:0},clearcoatRoughnessMap:{value:null},clearcoatRoughnessMapTransform:{value:new Ae},dispersion:{value:0},iridescence:{value:0},iridescenceMap:{value:null},iridescenceMapTransform:{value:new Ae},iridescenceIOR:{value:1.3},iridescenceThicknessMinimum:{value:100},iridescenceThicknessMaximum:{value:400},iridescenceThicknessMap:{value:null},iridescenceThicknessMapTransform:{value:new Ae},sheen:{value:0},sheenColor:{value:new Ze(0)},sheenColorMap:{value:null},sheenColorMapTransform:{value:new Ae},sheenRoughness:{value:1},sheenRoughnessMap:{value:null},sheenRoughnessMapTransform:{value:new Ae},transmission:{value:0},transmissionMap:{value:null},transmissionMapTransform:{value:new Ae},transmissionSamplerSize:{value:new tt},transmissionSamplerMap:{value:null},thickness:{value:0},thicknessMap:{value:null},thicknessMapTransform:{value:new Ae},attenuationDistance:{value:0},attenuationColor:{value:new Ze(0)},specularColor:{value:new Ze(1,1,1)},specularColorMap:{value:null},specularColorMapTransform:{value:new Ae},specularIntensity:{value:1},specularIntensityMap:{value:null},specularIntensityMapTransform:{value:new Ae},anisotropyVector:{value:new tt},anisotropyMap:{value:null},anisotropyMapTransform:{value:new Ae}}]),vertexShader:ke.meshphysical_vert,fragmentShader:ke.meshphysical_frag};var du={r:0,b:0,g:0},mI=new Tt,U_=new Ae;U_.set(-1,0,0,0,1,0,0,0,1);function gI(n,e,t,i,r,s){let o=new Ze(0),a=r===!0?0:1,c,l,u=null,f=0,d=null;function h(S){let T=S.isScene===!0?S.background:null;if(T&&T.isTexture){let E=S.backgroundBlurriness>0;T=e.get(T,E)}return T}function v(S){let T=!1,E=h(S);E===null?m(o,a):E&&E.isColor&&(m(E,1),T=!0);let D=n.xr.getEnvironmentBlendMode();D==="additive"?t.buffers.color.setClear(0,0,0,1,s):D==="alpha-blend"&&t.buffers.color.setClear(0,0,0,0,s),(n.autoClear||T)&&(t.buffers.depth.setTest(!0),t.buffers.depth.setMask(!0),t.buffers.color.setMask(!0),n.clear(n.autoClearColor,n.autoClearDepth,n.autoClearStencil))}function M(S,T){let E=h(T);E&&(E.isCubeTexture||E.mapping===Yo)?(l===void 0&&(l=new Qt(new Rs(1,1,1),new fn({name:"BackgroundCubeMaterial",uniforms:Nr(ni.backgroundCube.uniforms),vertexShader:ni.backgroundCube.vertexShader,fragmentShader:ni.backgroundCube.fragmentShader,side:qt,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),l.geometry.deleteAttribute("normal"),l.geometry.deleteAttribute("uv"),l.onBeforeRender=function(D,b,A){this.matrixWorld.copyPosition(A.matrixWorld)},Object.defineProperty(l.material,"envMap",{get:function(){return this.uniforms.envMap.value}}),i.update(l)),l.material.uniforms.envMap.value=E,l.material.uniforms.backgroundBlurriness.value=T.backgroundBlurriness,l.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,l.material.uniforms.backgroundRotation.value.setFromMatrix4(mI.makeRotationFromEuler(T.backgroundRotation)).transpose(),E.isCubeTexture&&E.isRenderTargetTexture===!1&&l.material.uniforms.backgroundRotation.value.premultiply(U_),l.material.toneMapped=ze.getTransfer(E.colorSpace)!==Qe,(u!==E||f!==E.version||d!==n.toneMapping)&&(l.material.needsUpdate=!0,u=E,f=E.version,d=n.toneMapping),l.layers.enableAll(),S.unshift(l,l.geometry,l.material,0,0,null)):E&&E.isTexture&&(c===void 0&&(c=new Qt(new jo(2,2),new fn({name:"BackgroundMaterial",uniforms:Nr(ni.background.uniforms),vertexShader:ni.background.vertexShader,fragmentShader:ni.background.fragmentShader,side:gi,depthTest:!1,depthWrite:!1,fog:!1,allowOverride:!1})),c.geometry.deleteAttribute("normal"),Object.defineProperty(c.material,"map",{get:function(){return this.uniforms.t2D.value}}),i.update(c)),c.material.uniforms.t2D.value=E,c.material.uniforms.backgroundIntensity.value=T.backgroundIntensity,c.material.toneMapped=ze.getTransfer(E.colorSpace)!==Qe,E.matrixAutoUpdate===!0&&E.updateMatrix(),c.material.uniforms.uvTransform.value.copy(E.matrix),(u!==E||f!==E.version||d!==n.toneMapping)&&(c.material.needsUpdate=!0,u=E,f=E.version,d=n.toneMapping),c.layers.enableAll(),S.unshift(c,c.geometry,c.material,0,0,null))}function m(S,T){S.getRGB(du,cp(n)),t.buffers.color.setClear(du.r,du.g,du.b,T,s)}function p(){l!==void 0&&(l.geometry.dispose(),l.material.dispose(),l=void 0),c!==void 0&&(c.geometry.dispose(),c.material.dispose(),c=void 0)}return{getClearColor:function(){return o},setClearColor:function(S,T=1){o.set(S),a=T,m(o,a)},getClearAlpha:function(){return a},setClearAlpha:function(S){a=S,m(o,a)},render:v,addToRenderList:M,dispose:p}}function vI(n,e){let t=n.getParameter(n.MAX_VERTEX_ATTRIBS),i={},r=d(null),s=r,o=!1;function a(I,k,W,X,P){let z=!1,F=f(I,X,W,k);s!==F&&(s=F,l(s.object)),z=h(I,X,W,P),z&&v(I,X,W,P),P!==null&&e.update(P,n.ELEMENT_ARRAY_BUFFER),(z||o)&&(o=!1,E(I,k,W,X),P!==null&&n.bindBuffer(n.ELEMENT_ARRAY_BUFFER,e.get(P).buffer))}function c(){return n.createVertexArray()}function l(I){return n.bindVertexArray(I)}function u(I){return n.deleteVertexArray(I)}function f(I,k,W,X){let P=X.wireframe===!0,z=i[k.id];z===void 0&&(z={},i[k.id]=z);let F=I.isInstancedMesh===!0?I.id:0,K=z[F];K===void 0&&(K={},z[F]=K);let Q=K[W.id];Q===void 0&&(Q={},K[W.id]=Q);let le=Q[P];return le===void 0&&(le=d(c()),Q[P]=le),le}function d(I){let k=[],W=[],X=[];for(let P=0;P<t;P++)k[P]=0,W[P]=0,X[P]=0;return{geometry:null,program:null,wireframe:!1,newAttributes:k,enabledAttributes:W,attributeDivisors:X,object:I,attributes:{},index:null}}function h(I,k,W,X){let P=s.attributes,z=k.attributes,F=0,K=W.getAttributes();for(let Q in K)if(K[Q].location>=0){let ye=P[Q],Se=z[Q];if(Se===void 0&&(Q==="instanceMatrix"&&I.instanceMatrix&&(Se=I.instanceMatrix),Q==="instanceColor"&&I.instanceColor&&(Se=I.instanceColor)),ye===void 0||ye.attribute!==Se||Se&&ye.data!==Se.data)return!0;F++}return s.attributesNum!==F||s.index!==X}function v(I,k,W,X){let P={},z=k.attributes,F=0,K=W.getAttributes();for(let Q in K)if(K[Q].location>=0){let ye=z[Q];ye===void 0&&(Q==="instanceMatrix"&&I.instanceMatrix&&(ye=I.instanceMatrix),Q==="instanceColor"&&I.instanceColor&&(ye=I.instanceColor));let Se={};Se.attribute=ye,ye&&ye.data&&(Se.data=ye.data),P[Q]=Se,F++}s.attributes=P,s.attributesNum=F,s.index=X}function M(){let I=s.newAttributes;for(let k=0,W=I.length;k<W;k++)I[k]=0}function m(I){p(I,0)}function p(I,k){let W=s.newAttributes,X=s.enabledAttributes,P=s.attributeDivisors;W[I]=1,X[I]===0&&(n.enableVertexAttribArray(I),X[I]=1),P[I]!==k&&(n.vertexAttribDivisor(I,k),P[I]=k)}function S(){let I=s.newAttributes,k=s.enabledAttributes;for(let W=0,X=k.length;W<X;W++)k[W]!==I[W]&&(n.disableVertexAttribArray(W),k[W]=0)}function T(I,k,W,X,P,z,F){F===!0?n.vertexAttribIPointer(I,k,W,P,z):n.vertexAttribPointer(I,k,W,X,P,z)}function E(I,k,W,X){M();let P=X.attributes,z=W.getAttributes(),F=k.defaultAttributeValues;for(let K in z){let Q=z[K];if(Q.location>=0){let le=P[K];if(le===void 0&&(K==="instanceMatrix"&&I.instanceMatrix&&(le=I.instanceMatrix),K==="instanceColor"&&I.instanceColor&&(le=I.instanceColor)),le!==void 0){let ye=le.normalized,Se=le.itemSize,qe=e.get(le);if(qe===void 0)continue;let nt=qe.buffer,Oe=qe.type,Y=qe.bytesPerElement,fe=Oe===n.INT||Oe===n.UNSIGNED_INT||le.gpuType===Il;if(le.isInterleavedBufferAttribute){let ie=le.data,Te=ie.stride,De=le.offset;if(ie.isInstancedInterleavedBuffer){for(let Ie=0;Ie<Q.locationSize;Ie++)p(Q.location+Ie,ie.meshPerAttribute);I.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=ie.meshPerAttribute*ie.count)}else for(let Ie=0;Ie<Q.locationSize;Ie++)m(Q.location+Ie);n.bindBuffer(n.ARRAY_BUFFER,nt);for(let Ie=0;Ie<Q.locationSize;Ie++)T(Q.location+Ie,Se/Q.locationSize,Oe,ye,Te*Y,(De+Se/Q.locationSize*Ie)*Y,fe)}else{if(le.isInstancedBufferAttribute){for(let ie=0;ie<Q.locationSize;ie++)p(Q.location+ie,le.meshPerAttribute);I.isInstancedMesh!==!0&&X._maxInstanceCount===void 0&&(X._maxInstanceCount=le.meshPerAttribute*le.count)}else for(let ie=0;ie<Q.locationSize;ie++)m(Q.location+ie);n.bindBuffer(n.ARRAY_BUFFER,nt);for(let ie=0;ie<Q.locationSize;ie++)T(Q.location+ie,Se/Q.locationSize,Oe,ye,Se*Y,Se/Q.locationSize*ie*Y,fe)}}else if(F!==void 0){let ye=F[K];if(ye!==void 0)switch(ye.length){case 2:n.vertexAttrib2fv(Q.location,ye);break;case 3:n.vertexAttrib3fv(Q.location,ye);break;case 4:n.vertexAttrib4fv(Q.location,ye);break;default:n.vertexAttrib1fv(Q.location,ye)}}}}S()}function D(){w();for(let I in i){let k=i[I];for(let W in k){let X=k[W];for(let P in X){let z=X[P];for(let F in z)u(z[F].object),delete z[F];delete X[P]}}delete i[I]}}function b(I){if(i[I.id]===void 0)return;let k=i[I.id];for(let W in k){let X=k[W];for(let P in X){let z=X[P];for(let F in z)u(z[F].object),delete z[F];delete X[P]}}delete i[I.id]}function A(I){for(let k in i){let W=i[k];for(let X in W){let P=W[X];if(P[I.id]===void 0)continue;let z=P[I.id];for(let F in z)u(z[F].object),delete z[F];delete P[I.id]}}}function y(I){for(let k in i){let W=i[k],X=I.isInstancedMesh===!0?I.id:0,P=W[X];if(P!==void 0){for(let z in P){let F=P[z];for(let K in F)u(F[K].object),delete F[K];delete P[z]}delete W[X],Object.keys(W).length===0&&delete i[k]}}}function w(){L(),o=!0,s!==r&&(s=r,l(s.object))}function L(){r.geometry=null,r.program=null,r.wireframe=!1}return{setup:a,reset:w,resetDefaultState:L,dispose:D,releaseStatesOfGeometry:b,releaseStatesOfObject:y,releaseStatesOfProgram:A,initAttributes:M,enableAttribute:m,disableUnusedAttributes:S}}function yI(n,e,t){let i;function r(c){i=c}function s(c,l){n.drawArrays(i,c,l),t.update(l,i,1)}function o(c,l,u){u!==0&&(n.drawArraysInstanced(i,c,l,u),t.update(l,i,u))}function a(c,l,u){if(u===0)return;e.get("WEBGL_multi_draw").multiDrawArraysWEBGL(i,c,0,l,0,u);let d=0;for(let h=0;h<u;h++)d+=l[h];t.update(d,i,1)}this.setMode=r,this.render=s,this.renderInstances=o,this.renderMultiDraw=a}function _I(n,e,t,i){let r;function s(){if(r!==void 0)return r;if(e.has("EXT_texture_filter_anisotropic")===!0){let A=e.get("EXT_texture_filter_anisotropic");r=n.getParameter(A.MAX_TEXTURE_MAX_ANISOTROPY_EXT)}else r=0;return r}function o(A){return!(A!==bn&&i.convert(A)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_FORMAT))}function a(A){let y=A===ei&&(e.has("EXT_color_buffer_half_float")||e.has("EXT_color_buffer_float"));return!(A!==pn&&i.convert(A)!==n.getParameter(n.IMPLEMENTATION_COLOR_READ_TYPE)&&A!==Un&&!y)}function c(A){if(A==="highp"){if(n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.HIGH_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.HIGH_FLOAT).precision>0)return"highp";A="mediump"}return A==="mediump"&&n.getShaderPrecisionFormat(n.VERTEX_SHADER,n.MEDIUM_FLOAT).precision>0&&n.getShaderPrecisionFormat(n.FRAGMENT_SHADER,n.MEDIUM_FLOAT).precision>0?"mediump":"lowp"}let l=t.precision!==void 0?t.precision:"highp",u=c(l);u!==l&&(be("WebGLRenderer:",l,"not supported, using",u,"instead."),l=u);let f=t.logarithmicDepthBuffer===!0,d=t.reversedDepthBuffer===!0&&e.has("EXT_clip_control");t.reversedDepthBuffer===!0&&d===!1&&be("WebGLRenderer: Unable to use reversed depth buffer due to missing EXT_clip_control extension. Fallback to default depth buffer.");let h=n.getParameter(n.MAX_TEXTURE_IMAGE_UNITS),v=n.getParameter(n.MAX_VERTEX_TEXTURE_IMAGE_UNITS),M=n.getParameter(n.MAX_TEXTURE_SIZE),m=n.getParameter(n.MAX_CUBE_MAP_TEXTURE_SIZE),p=n.getParameter(n.MAX_VERTEX_ATTRIBS),S=n.getParameter(n.MAX_VERTEX_UNIFORM_VECTORS),T=n.getParameter(n.MAX_VARYING_VECTORS),E=n.getParameter(n.MAX_FRAGMENT_UNIFORM_VECTORS),D=n.getParameter(n.MAX_SAMPLES),b=n.getParameter(n.SAMPLES);return{isWebGL2:!0,getMaxAnisotropy:s,getMaxPrecision:c,textureFormatReadable:o,textureTypeReadable:a,precision:l,logarithmicDepthBuffer:f,reversedDepthBuffer:d,maxTextures:h,maxVertexTextures:v,maxTextureSize:M,maxCubemapSize:m,maxAttributes:p,maxVertexUniforms:S,maxVaryings:T,maxFragmentUniforms:E,maxSamples:D,samples:b}}function xI(n){let e=this,t=null,i=0,r=!1,s=!1,o=new Xn,a=new Ae,c={value:null,needsUpdate:!1};this.uniform=c,this.numPlanes=0,this.numIntersection=0,this.init=function(f,d){let h=f.length!==0||d||i!==0||r;return r=d,i=f.length,h},this.beginShadows=function(){s=!0,u(null)},this.endShadows=function(){s=!1},this.setGlobalState=function(f,d){t=u(f,d,0)},this.setState=function(f,d,h){let v=f.clippingPlanes,M=f.clipIntersection,m=f.clipShadows,p=n.get(f);if(!r||v===null||v.length===0||s&&!m)s?u(null):l();else{let S=s?0:i,T=S*4,E=p.clippingState||null;c.value=E,E=u(v,d,T,h);for(let D=0;D!==T;++D)E[D]=t[D];p.clippingState=E,this.numIntersection=M?this.numPlanes:0,this.numPlanes+=S}};function l(){c.value!==t&&(c.value=t,c.needsUpdate=i>0),e.numPlanes=i,e.numIntersection=0}function u(f,d,h,v){let M=f!==null?f.length:0,m=null;if(M!==0){if(m=c.value,v!==!0||m===null){let p=h+M*4,S=d.matrixWorldInverse;a.getNormalMatrix(S),(m===null||m.length<p)&&(m=new Float32Array(p));for(let T=0,E=h;T!==M;++T,E+=4)o.copy(f[T]).applyMatrix4(S,a),o.normal.toArray(m,E),m[E+3]=o.constant}c.value=m,c.needsUpdate=!0}return e.numPlanes=M,e.numIntersection=0,m}}var Yi=4,m_=[.125,.215,.35,.446,.526,.582],Pr=20,MI=256,ra=new Ar,g_=new Ze,fp=null,hp=0,pp=0,mp=!1,SI=new V,hu=class{constructor(e){this._renderer=e,this._pingPongRenderTarget=null,this._lodMax=0,this._cubeSize=0,this._sizeLods=[],this._sigmas=[],this._lodMeshes=[],this._backgroundBox=null,this._cubemapMaterial=null,this._equirectMaterial=null,this._blurMaterial=null,this._ggxMaterial=null}fromScene(e,t=0,i=.1,r=100,s={}){let{size:o=256,position:a=SI}=s;fp=this._renderer.getRenderTarget(),hp=this._renderer.getActiveCubeFace(),pp=this._renderer.getActiveMipmapLevel(),mp=this._renderer.xr.enabled,this._renderer.xr.enabled=!1,this._setSize(o);let c=this._allocateTargets();return c.depthBuffer=!0,this._sceneToCubeUV(e,i,r,c,a),t>0&&this._blur(c,0,0,t),this._applyPMREM(c),this._cleanup(c),c}fromEquirectangular(e,t=null){return this._fromTexture(e,t)}fromCubemap(e,t=null){return this._fromTexture(e,t)}compileCubemapShader(){this._cubemapMaterial===null&&(this._cubemapMaterial=__(),this._compileMaterial(this._cubemapMaterial))}compileEquirectangularShader(){this._equirectMaterial===null&&(this._equirectMaterial=y_(),this._compileMaterial(this._equirectMaterial))}dispose(){this._dispose(),this._cubemapMaterial!==null&&this._cubemapMaterial.dispose(),this._equirectMaterial!==null&&this._equirectMaterial.dispose(),this._backgroundBox!==null&&(this._backgroundBox.geometry.dispose(),this._backgroundBox.material.dispose())}_setSize(e){this._lodMax=Math.floor(Math.log2(e)),this._cubeSize=Math.pow(2,this._lodMax)}_dispose(){this._blurMaterial!==null&&this._blurMaterial.dispose(),this._ggxMaterial!==null&&this._ggxMaterial.dispose(),this._pingPongRenderTarget!==null&&this._pingPongRenderTarget.dispose();for(let e=0;e<this._lodMeshes.length;e++)this._lodMeshes[e].geometry.dispose()}_cleanup(e){this._renderer.setRenderTarget(fp,hp,pp),this._renderer.xr.enabled=mp,e.scissorTest=!1,Fs(e,0,0,e.width,e.height)}_fromTexture(e,t){e.mapping===ji||e.mapping===Dr?this._setSize(e.image.length===0?16:e.image[0].width||e.image[0].image.width):this._setSize(e.image.width/4),fp=this._renderer.getRenderTarget(),hp=this._renderer.getActiveCubeFace(),pp=this._renderer.getActiveMipmapLevel(),mp=this._renderer.xr.enabled,this._renderer.xr.enabled=!1;let i=t||this._allocateTargets();return this._textureToCubeUV(e,i),this._applyPMREM(i),this._cleanup(i),i}_allocateTargets(){let e=3*Math.max(this._cubeSize,112),t=4*this._cubeSize,i={magFilter:Ot,minFilter:Ot,generateMipmaps:!1,type:ei,format:bn,colorSpace:Ro,depthBuffer:!1},r=v_(e,t,i);if(this._pingPongRenderTarget===null||this._pingPongRenderTarget.width!==e||this._pingPongRenderTarget.height!==t){this._pingPongRenderTarget!==null&&this._dispose(),this._pingPongRenderTarget=v_(e,t,i);let{_lodMax:s}=this;({lodMeshes:this._lodMeshes,sizeLods:this._sizeLods,sigmas:this._sigmas}=EI(s)),this._blurMaterial=TI(s,e,t),this._ggxMaterial=bI(s,e,t)}return r}_compileMaterial(e){let t=new Qt(new Sn,e);this._renderer.compile(t,ra)}_sceneToCubeUV(e,t,i,r,s){let c=new Kt(90,1,t,i),l=[1,-1,1,1,1,1],u=[1,1,1,-1,-1,-1],f=this._renderer,d=f.autoClear,h=f.toneMapping;f.getClearColor(g_),f.toneMapping=Fn,f.autoClear=!1,f.state.buffers.depth.getReversed()&&(f.setRenderTarget(r),f.clearDepth(),f.setRenderTarget(null)),this._backgroundBox===null&&(this._backgroundBox=new Qt(new Rs,new Ho({name:"PMREM.Background",side:qt,depthWrite:!1,depthTest:!1})));let M=this._backgroundBox,m=M.material,p=!1,S=e.background;S?S.isColor&&(m.color.copy(S),e.background=null,p=!0):(m.color.copy(g_),p=!0);for(let T=0;T<6;T++){let E=T%3;E===0?(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x+u[T],s.y,s.z)):E===1?(c.up.set(0,0,l[T]),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y+u[T],s.z)):(c.up.set(0,l[T],0),c.position.set(s.x,s.y,s.z),c.lookAt(s.x,s.y,s.z+u[T]));let D=this._cubeSize;Fs(r,E*D,T>2?D:0,D,D),f.setRenderTarget(r),p&&f.render(M,c),f.render(e,c)}f.toneMapping=h,f.autoClear=d,e.background=S}_textureToCubeUV(e,t){let i=this._renderer,r=e.mapping===ji||e.mapping===Dr;r?(this._cubemapMaterial===null&&(this._cubemapMaterial=__()),this._cubemapMaterial.uniforms.flipEnvMap.value=e.isRenderTargetTexture===!1?-1:1):this._equirectMaterial===null&&(this._equirectMaterial=y_());let s=r?this._cubemapMaterial:this._equirectMaterial,o=this._lodMeshes[0];o.material=s;let a=s.uniforms;a.envMap.value=e;let c=this._cubeSize;Fs(t,0,0,3*c,2*c),i.setRenderTarget(t),i.render(o,ra)}_applyPMREM(e){let t=this._renderer,i=t.autoClear;t.autoClear=!1;let r=this._lodMeshes.length;for(let s=1;s<r;s++)this._applyGGXFilter(e,s-1,s);t.autoClear=i}_applyGGXFilter(e,t,i){let r=this._renderer,s=this._pingPongRenderTarget,o=this._ggxMaterial,a=this._lodMeshes[i];a.material=o;let c=o.uniforms,l=i/(this._lodMeshes.length-1),u=t/(this._lodMeshes.length-1),f=Math.sqrt(l*l-u*u),d=0+l*1.25,h=f*d,{_lodMax:v}=this,M=this._sizeLods[i],m=3*M*(i>v-Yi?i-v+Yi:0),p=4*(this._cubeSize-M);c.envMap.value=e.texture,c.roughness.value=h,c.mipInt.value=v-t,Fs(s,m,p,3*M,2*M),r.setRenderTarget(s),r.render(a,ra),c.envMap.value=s.texture,c.roughness.value=0,c.mipInt.value=v-i,Fs(e,m,p,3*M,2*M),r.setRenderTarget(e),r.render(a,ra)}_blur(e,t,i,r,s){let o=this._pingPongRenderTarget;this._halfBlur(e,o,t,i,r,"latitudinal",s),this._halfBlur(o,e,i,i,r,"longitudinal",s)}_halfBlur(e,t,i,r,s,o,a){let c=this._renderer,l=this._blurMaterial;o!=="latitudinal"&&o!=="longitudinal"&&we("blur direction must be either latitudinal or longitudinal!");let u=3,f=this._lodMeshes[r];f.material=l;let d=l.uniforms,h=this._sizeLods[i]-1,v=isFinite(s)?Math.PI/(2*h):2*Math.PI/(2*Pr-1),M=s/v,m=isFinite(s)?1+Math.floor(u*M):Pr;m>Pr&&be(`sigmaRadians, ${s}, is too large and will clip, as it requested ${m} samples when the maximum is set to ${Pr}`);let p=[],S=0;for(let A=0;A<Pr;++A){let y=A/M,w=Math.exp(-y*y/2);p.push(w),A===0?S+=w:A<m&&(S+=2*w)}for(let A=0;A<p.length;A++)p[A]=p[A]/S;d.envMap.value=e.texture,d.samples.value=m,d.weights.value=p,d.latitudinal.value=o==="latitudinal",a&&(d.poleAxis.value=a);let{_lodMax:T}=this;d.dTheta.value=v,d.mipInt.value=T-i;let E=this._sizeLods[r],D=3*E*(r>T-Yi?r-T+Yi:0),b=4*(this._cubeSize-E);Fs(t,D,b,3*E,2*E),c.setRenderTarget(t),c.render(f,ra)}};function EI(n){let e=[],t=[],i=[],r=n,s=n-Yi+1+m_.length;for(let o=0;o<s;o++){let a=Math.pow(2,r);e.push(a);let c=1/a;o>n-Yi?c=m_[o-n+Yi-1]:o===0&&(c=0),t.push(c);let l=1/(a-2),u=-l,f=1+l,d=[u,u,f,u,f,f,u,u,f,f,u,f],h=6,v=6,M=3,m=2,p=1,S=new Float32Array(M*v*h),T=new Float32Array(m*v*h),E=new Float32Array(p*v*h);for(let b=0;b<h;b++){let A=b%3*2/3-1,y=b>2?0:-1,w=[A,y,0,A+2/3,y,0,A+2/3,y+1,0,A,y,0,A+2/3,y+1,0,A,y+1,0];S.set(w,M*v*b),T.set(d,m*v*b);let L=[b,b,b,b,b,b];E.set(L,p*v*b)}let D=new Sn;D.setAttribute("position",new $t(S,M)),D.setAttribute("uv",new $t(T,m)),D.setAttribute("faceIndex",new $t(E,p)),i.push(new Qt(D,null)),r>Yi&&r--}return{lodMeshes:i,sizeLods:e,sigmas:t}}function v_(n,e,t){let i=new dn(n,e,t);return i.texture.mapping=Yo,i.texture.name="PMREM.cubeUv",i.scissorTest=!0,i}function Fs(n,e,t,i,r){n.viewport.set(e,t,i,r),n.scissor.set(e,t,i,r)}function bI(n,e,t){return new fn({name:"PMREMGGXConvolution",defines:{GGX_SAMPLES:MI,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},roughness:{value:0},mipInt:{value:0}},vertexShader:gu(),fragmentShader:`

			precision highp float;
			precision highp int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform float roughness;
			uniform float mipInt;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			#define PI 3.14159265359

			// Van der Corput radical inverse
			float radicalInverse_VdC(uint bits) {
				bits = (bits << 16u) | (bits >> 16u);
				bits = ((bits & 0x55555555u) << 1u) | ((bits & 0xAAAAAAAAu) >> 1u);
				bits = ((bits & 0x33333333u) << 2u) | ((bits & 0xCCCCCCCCu) >> 2u);
				bits = ((bits & 0x0F0F0F0Fu) << 4u) | ((bits & 0xF0F0F0F0u) >> 4u);
				bits = ((bits & 0x00FF00FFu) << 8u) | ((bits & 0xFF00FF00u) >> 8u);
				return float(bits) * 2.3283064365386963e-10; // / 0x100000000
			}

			// Hammersley sequence
			vec2 hammersley(uint i, uint N) {
				return vec2(float(i) / float(N), radicalInverse_VdC(i));
			}

			// GGX VNDF importance sampling (Eric Heitz 2018)
			// "Sampling the GGX Distribution of Visible Normals"
			// https://jcgt.org/published/0007/04/01/
			vec3 importanceSampleGGX_VNDF(vec2 Xi, vec3 V, float roughness) {
				float alpha = roughness * roughness;

				// Section 4.1: Orthonormal basis
				vec3 T1 = vec3(1.0, 0.0, 0.0);
				vec3 T2 = cross(V, T1);

				// Section 4.2: Parameterization of projected area
				float r = sqrt(Xi.x);
				float phi = 2.0 * PI * Xi.y;
				float t1 = r * cos(phi);
				float t2 = r * sin(phi);
				float s = 0.5 * (1.0 + V.z);
				t2 = (1.0 - s) * sqrt(1.0 - t1 * t1) + s * t2;

				// Section 4.3: Reprojection onto hemisphere
				vec3 Nh = t1 * T1 + t2 * T2 + sqrt(max(0.0, 1.0 - t1 * t1 - t2 * t2)) * V;

				// Section 3.4: Transform back to ellipsoid configuration
				return normalize(vec3(alpha * Nh.x, alpha * Nh.y, max(0.0, Nh.z)));
			}

			void main() {
				vec3 N = normalize(vOutputDirection);
				vec3 V = N; // Assume view direction equals normal for pre-filtering

				vec3 prefilteredColor = vec3(0.0);
				float totalWeight = 0.0;

				// For very low roughness, just sample the environment directly
				if (roughness < 0.001) {
					gl_FragColor = vec4(bilinearCubeUV(envMap, N, mipInt), 1.0);
					return;
				}

				// Tangent space basis for VNDF sampling
				vec3 up = abs(N.z) < 0.999 ? vec3(0.0, 0.0, 1.0) : vec3(1.0, 0.0, 0.0);
				vec3 tangent = normalize(cross(up, N));
				vec3 bitangent = cross(N, tangent);

				for(uint i = 0u; i < uint(GGX_SAMPLES); i++) {
					vec2 Xi = hammersley(i, uint(GGX_SAMPLES));

					// For PMREM, V = N, so in tangent space V is always (0, 0, 1)
					vec3 H_tangent = importanceSampleGGX_VNDF(Xi, vec3(0.0, 0.0, 1.0), roughness);

					// Transform H back to world space
					vec3 H = normalize(tangent * H_tangent.x + bitangent * H_tangent.y + N * H_tangent.z);
					vec3 L = normalize(2.0 * dot(V, H) * H - V);

					float NdotL = max(dot(N, L), 0.0);

					if(NdotL > 0.0) {
						// Sample environment at fixed mip level
						// VNDF importance sampling handles the distribution filtering
						vec3 sampleColor = bilinearCubeUV(envMap, L, mipInt);

						// Weight by NdotL for the split-sum approximation
						// VNDF PDF naturally accounts for the visible microfacet distribution
						prefilteredColor += sampleColor * NdotL;
						totalWeight += NdotL;
					}
				}

				if (totalWeight > 0.0) {
					prefilteredColor = prefilteredColor / totalWeight;
				}

				gl_FragColor = vec4(prefilteredColor, 1.0);
			}
		`,blending:Qn,depthTest:!1,depthWrite:!1})}function TI(n,e,t){let i=new Float32Array(Pr),r=new V(0,1,0);return new fn({name:"SphericalGaussianBlur",defines:{n:Pr,CUBEUV_TEXEL_WIDTH:1/e,CUBEUV_TEXEL_HEIGHT:1/t,CUBEUV_MAX_MIP:`${n}.0`},uniforms:{envMap:{value:null},samples:{value:1},weights:{value:i},latitudinal:{value:!1},dTheta:{value:0},mipInt:{value:0},poleAxis:{value:r}},vertexShader:gu(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;
			uniform int samples;
			uniform float weights[ n ];
			uniform bool latitudinal;
			uniform float dTheta;
			uniform float mipInt;
			uniform vec3 poleAxis;

			#define ENVMAP_TYPE_CUBE_UV
			#include <cube_uv_reflection_fragment>

			vec3 getSample( float theta, vec3 axis ) {

				float cosTheta = cos( theta );
				// Rodrigues' axis-angle rotation
				vec3 sampleDirection = vOutputDirection * cosTheta
					+ cross( axis, vOutputDirection ) * sin( theta )
					+ axis * dot( axis, vOutputDirection ) * ( 1.0 - cosTheta );

				return bilinearCubeUV( envMap, sampleDirection, mipInt );

			}

			void main() {

				vec3 axis = latitudinal ? poleAxis : cross( poleAxis, vOutputDirection );

				if ( all( equal( axis, vec3( 0.0 ) ) ) ) {

					axis = vec3( vOutputDirection.z, 0.0, - vOutputDirection.x );

				}

				axis = normalize( axis );

				gl_FragColor = vec4( 0.0, 0.0, 0.0, 1.0 );
				gl_FragColor.rgb += weights[ 0 ] * getSample( 0.0, axis );

				for ( int i = 1; i < n; i++ ) {

					if ( i >= samples ) {

						break;

					}

					float theta = dTheta * float( i );
					gl_FragColor.rgb += weights[ i ] * getSample( -1.0 * theta, axis );
					gl_FragColor.rgb += weights[ i ] * getSample( theta, axis );

				}

			}
		`,blending:Qn,depthTest:!1,depthWrite:!1})}function y_(){return new fn({name:"EquirectangularToCubeUV",uniforms:{envMap:{value:null}},vertexShader:gu(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			varying vec3 vOutputDirection;

			uniform sampler2D envMap;

			#include <common>

			void main() {

				vec3 outputDirection = normalize( vOutputDirection );
				vec2 uv = equirectUv( outputDirection );

				gl_FragColor = vec4( texture2D ( envMap, uv ).rgb, 1.0 );

			}
		`,blending:Qn,depthTest:!1,depthWrite:!1})}function __(){return new fn({name:"CubemapToCubeUV",uniforms:{envMap:{value:null},flipEnvMap:{value:-1}},vertexShader:gu(),fragmentShader:`

			precision mediump float;
			precision mediump int;

			uniform float flipEnvMap;

			varying vec3 vOutputDirection;

			uniform samplerCube envMap;

			void main() {

				gl_FragColor = textureCube( envMap, vec3( flipEnvMap * vOutputDirection.x, vOutputDirection.yz ) );

			}
		`,blending:Qn,depthTest:!1,depthWrite:!1})}function gu(){return`

		precision mediump float;
		precision mediump int;

		attribute float faceIndex;

		varying vec3 vOutputDirection;

		// RH coordinate system; PMREM face-indexing convention
		vec3 getDirection( vec2 uv, float face ) {

			uv = 2.0 * uv - 1.0;

			vec3 direction = vec3( uv, 1.0 );

			if ( face == 0.0 ) {

				direction = direction.zyx; // ( 1, v, u ) pos x

			} else if ( face == 1.0 ) {

				direction = direction.xzy;
				direction.xz *= -1.0; // ( -u, 1, -v ) pos y

			} else if ( face == 2.0 ) {

				direction.x *= -1.0; // ( -u, v, 1 ) pos z

			} else if ( face == 3.0 ) {

				direction = direction.zyx;
				direction.xz *= -1.0; // ( -1, v, -u ) neg x

			} else if ( face == 4.0 ) {

				direction = direction.xzy;
				direction.xy *= -1.0; // ( -u, -1, v ) neg y

			} else if ( face == 5.0 ) {

				direction.z *= -1.0; // ( u, v, -1 ) neg z

			}

			return direction;

		}

		void main() {

			vOutputDirection = getDirection( uv, faceIndex );
			gl_Position = vec4( position, 1.0 );

		}
	`}var pu=class extends dn{constructor(e=1,t={}){super(e,e,t),this.isWebGLCubeRenderTarget=!0;let i={width:e,height:e,depth:1},r=[i,i,i,i,i,i];this.texture=new Go(r),this._setTextureOptions(t),this.texture.isRenderTargetTexture=!0}fromEquirectangularTexture(e,t){this.texture.type=t.type,this.texture.colorSpace=t.colorSpace,this.texture.generateMipmaps=t.generateMipmaps,this.texture.minFilter=t.minFilter,this.texture.magFilter=t.magFilter;let i={uniforms:{tEquirect:{value:null}},vertexShader:`

				varying vec3 vWorldDirection;

				vec3 transformDirection( in vec3 dir, in mat4 matrix ) {

					return normalize( ( matrix * vec4( dir, 0.0 ) ).xyz );

				}

				void main() {

					vWorldDirection = transformDirection( position, modelMatrix );

					#include <begin_vertex>
					#include <project_vertex>

				}
			`,fragmentShader:`

				uniform sampler2D tEquirect;

				varying vec3 vWorldDirection;

				#include <common>

				void main() {

					vec3 direction = normalize( vWorldDirection );

					vec2 sampleUV = equirectUv( direction );

					gl_FragColor = texture2D( tEquirect, sampleUV );

				}
			`},r=new Rs(5,5,5),s=new fn({name:"CubemapFromEquirect",uniforms:Nr(i.uniforms),vertexShader:i.vertexShader,fragmentShader:i.fragmentShader,side:qt,blending:Qn});s.uniforms.tEquirect.value=t;let o=new Qt(r,s),a=t.minFilter;return t.minFilter===$i&&(t.minFilter=Ot),new Sl(1,10,this).update(e,o),t.minFilter=a,o.geometry.dispose(),o.material.dispose(),this}clear(e,t=!0,i=!0,r=!0){let s=e.getRenderTarget();for(let o=0;o<6;o++)e.setRenderTarget(this,o),e.clear(t,i,r);e.setRenderTarget(s)}};function wI(n){let e=new WeakMap,t=new WeakMap,i=null;function r(d,h=!1){return d==null?null:h?o(d):s(d)}function s(d){if(d&&d.isTexture){let h=d.mapping;if(h===bl||h===Tl)if(e.has(d)){let v=e.get(d).texture;return a(v,d.mapping)}else{let v=d.image;if(v&&v.height>0){let M=new pu(v.height);return M.fromEquirectangularTexture(n,d),e.set(d,M),d.addEventListener("dispose",l),a(M.texture,d.mapping)}else return null}}return d}function o(d){if(d&&d.isTexture){let h=d.mapping,v=h===bl||h===Tl,M=h===ji||h===Dr;if(v||M){let m=t.get(d),p=m!==void 0?m.texture.pmremVersion:0;if(d.isRenderTargetTexture&&d.pmremVersion!==p)return i===null&&(i=new hu(n)),m=v?i.fromEquirectangular(d,m):i.fromCubemap(d,m),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),m.texture;if(m!==void 0)return m.texture;{let S=d.image;return v&&S&&S.height>0||M&&S&&c(S)?(i===null&&(i=new hu(n)),m=v?i.fromEquirectangular(d):i.fromCubemap(d),m.texture.pmremVersion=d.pmremVersion,t.set(d,m),d.addEventListener("dispose",u),m.texture):null}}}return d}function a(d,h){return h===bl?d.mapping=ji:h===Tl&&(d.mapping=Dr),d}function c(d){let h=0,v=6;for(let M=0;M<v;M++)d[M]!==void 0&&h++;return h===v}function l(d){let h=d.target;h.removeEventListener("dispose",l);let v=e.get(h);v!==void 0&&(e.delete(h),v.dispose())}function u(d){let h=d.target;h.removeEventListener("dispose",u);let v=t.get(h);v!==void 0&&(t.delete(h),v.dispose())}function f(){e=new WeakMap,t=new WeakMap,i!==null&&(i.dispose(),i=null)}return{get:r,dispose:f}}function II(n){let e={};function t(i){if(e[i]!==void 0)return e[i];let r=n.getExtension(i);return e[i]=r,r}return{has:function(i){return t(i)!==null},init:function(){t("EXT_color_buffer_float"),t("WEBGL_clip_cull_distance"),t("OES_texture_float_linear"),t("EXT_color_buffer_half_float"),t("WEBGL_multisampled_render_to_texture"),t("WEBGL_render_shared_exponent")},get:function(i){let r=t(i);return r===null&&rl("WebGLRenderer: "+i+" extension not supported."),r}}}function CI(n,e,t,i){let r={},s=new WeakMap;function o(f){let d=f.target;d.index!==null&&e.remove(d.index);for(let v in d.attributes)e.remove(d.attributes[v]);d.removeEventListener("dispose",o),delete r[d.id];let h=s.get(d);h&&(e.remove(h),s.delete(d)),i.releaseStatesOfGeometry(d),d.isInstancedBufferGeometry===!0&&delete d._maxInstanceCount,t.memory.geometries--}function a(f,d){return r[d.id]===!0||(d.addEventListener("dispose",o),r[d.id]=!0,t.memory.geometries++),d}function c(f){let d=f.attributes;for(let h in d)e.update(d[h],n.ARRAY_BUFFER)}function l(f){let d=[],h=f.index,v=f.attributes.position,M=0;if(v===void 0)return;if(h!==null){let S=h.array;M=h.version;for(let T=0,E=S.length;T<E;T+=3){let D=S[T+0],b=S[T+1],A=S[T+2];d.push(D,b,b,A,A,D)}}else{let S=v.array;M=v.version;for(let T=0,E=S.length/3-1;T<E;T+=3){let D=T+0,b=T+1,A=T+2;d.push(D,b,b,A,A,D)}}let m=new(v.count>=65535?Vo:Bo)(d,1);m.version=M;let p=s.get(f);p&&e.remove(p),s.set(f,m)}function u(f){let d=s.get(f);if(d){let h=f.index;h!==null&&d.version<h.version&&l(f)}else l(f);return s.get(f)}return{get:a,update:c,getWireframeAttribute:u}}function AI(n,e,t){let i;function r(f){i=f}let s,o;function a(f){s=f.type,o=f.bytesPerElement}function c(f,d){n.drawElements(i,d,s,f*o),t.update(d,i,1)}function l(f,d,h){h!==0&&(n.drawElementsInstanced(i,d,s,f*o,h),t.update(d,i,h))}function u(f,d,h){if(h===0)return;e.get("WEBGL_multi_draw").multiDrawElementsWEBGL(i,d,0,s,f,0,h);let M=0;for(let m=0;m<h;m++)M+=d[m];t.update(M,i,1)}this.setMode=r,this.setIndex=a,this.render=c,this.renderInstances=l,this.renderMultiDraw=u}function DI(n){let e={geometries:0,textures:0},t={frame:0,calls:0,triangles:0,points:0,lines:0};function i(s,o,a){switch(t.calls++,o){case n.TRIANGLES:t.triangles+=a*(s/3);break;case n.LINES:t.lines+=a*(s/2);break;case n.LINE_STRIP:t.lines+=a*(s-1);break;case n.LINE_LOOP:t.lines+=a*s;break;case n.POINTS:t.points+=a*s;break;default:we("WebGLInfo: Unknown draw mode:",o);break}}function r(){t.calls=0,t.triangles=0,t.points=0,t.lines=0}return{memory:e,render:t,programs:null,autoReset:!0,reset:r,update:i}}function RI(n,e,t){let i=new WeakMap,r=new _t;function s(o,a,c){let l=o.morphTargetInfluences,u=a.morphAttributes.position||a.morphAttributes.normal||a.morphAttributes.color,f=u!==void 0?u.length:0,d=i.get(a);if(d===void 0||d.count!==f){let L=function(){y.dispose(),i.delete(a),a.removeEventListener("dispose",L)};var h=L;d!==void 0&&d.texture.dispose();let v=a.morphAttributes.position!==void 0,M=a.morphAttributes.normal!==void 0,m=a.morphAttributes.color!==void 0,p=a.morphAttributes.position||[],S=a.morphAttributes.normal||[],T=a.morphAttributes.color||[],E=0;v===!0&&(E=1),M===!0&&(E=2),m===!0&&(E=3);let D=a.attributes.position.count*E,b=1;D>e.maxTextureSize&&(b=Math.ceil(D/e.maxTextureSize),D=e.maxTextureSize);let A=new Float32Array(D*b*4*f),y=new Oo(A,D,b,f);y.type=Un,y.needsUpdate=!0;let w=E*4;for(let I=0;I<f;I++){let k=p[I],W=S[I],X=T[I],P=D*b*4*I;for(let z=0;z<k.count;z++){let F=z*w;v===!0&&(r.fromBufferAttribute(k,z),A[P+F+0]=r.x,A[P+F+1]=r.y,A[P+F+2]=r.z,A[P+F+3]=0),M===!0&&(r.fromBufferAttribute(W,z),A[P+F+4]=r.x,A[P+F+5]=r.y,A[P+F+6]=r.z,A[P+F+7]=0),m===!0&&(r.fromBufferAttribute(X,z),A[P+F+8]=r.x,A[P+F+9]=r.y,A[P+F+10]=r.z,A[P+F+11]=X.itemSize===4?r.w:1)}}d={count:f,texture:y,size:new tt(D,b)},i.set(a,d),a.addEventListener("dispose",L)}if(o.isInstancedMesh===!0&&o.morphTexture!==null)c.getUniforms().setValue(n,"morphTexture",o.morphTexture,t);else{let v=0;for(let m=0;m<l.length;m++)v+=l[m];let M=a.morphTargetsRelative?1:1-v;c.getUniforms().setValue(n,"morphTargetBaseInfluence",M),c.getUniforms().setValue(n,"morphTargetInfluences",l)}c.getUniforms().setValue(n,"morphTargetsTexture",d.texture,t),c.getUniforms().setValue(n,"morphTargetsTextureSize",d.size)}return{update:s}}function NI(n,e,t,i,r){let s=new WeakMap;function o(l){let u=r.render.frame,f=l.geometry,d=e.get(l,f);if(s.get(d)!==u&&(e.update(d),s.set(d,u)),l.isInstancedMesh&&(l.hasEventListener("dispose",c)===!1&&l.addEventListener("dispose",c),s.get(l)!==u&&(t.update(l.instanceMatrix,n.ARRAY_BUFFER),l.instanceColor!==null&&t.update(l.instanceColor,n.ARRAY_BUFFER),s.set(l,u))),l.isSkinnedMesh){let h=l.skeleton;s.get(h)!==u&&(h.update(),s.set(h,u))}return d}function a(){s=new WeakMap}function c(l){let u=l.target;u.removeEventListener("dispose",c),i.releaseStatesOfObject(u),t.remove(u.instanceMatrix),u.instanceColor!==null&&t.remove(u.instanceColor)}return{update:o,dispose:a}}var PI={[jh]:"LINEAR_TONE_MAPPING",[$h]:"REINHARD_TONE_MAPPING",[qh]:"CINEON_TONE_MAPPING",[Xh]:"ACES_FILMIC_TONE_MAPPING",[Zh]:"AGX_TONE_MAPPING",[Jh]:"NEUTRAL_TONE_MAPPING",[Yh]:"CUSTOM_TONE_MAPPING"};function LI(n,e,t,i,r){let s=new dn(e,t,{type:n,depthBuffer:i,stencilBuffer:r,depthTexture:i?new yi(e,t):void 0}),o=new dn(e,t,{type:ei,depthBuffer:!1,stencilBuffer:!1}),a=new Sn;a.setAttribute("position",new Mn([-1,3,0,-1,-1,0,3,-1,0],3)),a.setAttribute("uv",new Mn([0,2,0,0,2,0],2));let c=new Ns({uniforms:{tDiffuse:{value:null}},vertexShader:`
			precision highp float;

			uniform mat4 modelViewMatrix;
			uniform mat4 projectionMatrix;

			attribute vec3 position;
			attribute vec2 uv;

			varying vec2 vUv;

			void main() {
				vUv = uv;
				gl_Position = projectionMatrix * modelViewMatrix * vec4( position, 1.0 );
			}`,fragmentShader:`
			precision highp float;

			uniform sampler2D tDiffuse;

			varying vec2 vUv;

			#include <tonemapping_pars_fragment>
			#include <colorspace_pars_fragment>

			void main() {
				gl_FragColor = texture2D( tDiffuse, vUv );

				#ifdef LINEAR_TONE_MAPPING
					gl_FragColor.rgb = LinearToneMapping( gl_FragColor.rgb );
				#elif defined( REINHARD_TONE_MAPPING )
					gl_FragColor.rgb = ReinhardToneMapping( gl_FragColor.rgb );
				#elif defined( CINEON_TONE_MAPPING )
					gl_FragColor.rgb = CineonToneMapping( gl_FragColor.rgb );
				#elif defined( ACES_FILMIC_TONE_MAPPING )
					gl_FragColor.rgb = ACESFilmicToneMapping( gl_FragColor.rgb );
				#elif defined( AGX_TONE_MAPPING )
					gl_FragColor.rgb = AgXToneMapping( gl_FragColor.rgb );
				#elif defined( NEUTRAL_TONE_MAPPING )
					gl_FragColor.rgb = NeutralToneMapping( gl_FragColor.rgb );
				#elif defined( CUSTOM_TONE_MAPPING )
					gl_FragColor.rgb = CustomToneMapping( gl_FragColor.rgb );
				#endif

				#ifdef SRGB_TRANSFER
					gl_FragColor = sRGBTransferOETF( gl_FragColor );
				#endif
			}`,depthTest:!1,depthWrite:!1}),l=new Qt(a,c),u=new Ar(-1,1,1,-1,0,1),f=null,d=null,h=!1,v,M=null,m=[],p=!1;this.setSize=function(S,T){s.setSize(S,T),o.setSize(S,T);for(let E=0;E<m.length;E++){let D=m[E];D.setSize&&D.setSize(S,T)}},this.setEffects=function(S){m=S,p=m.length>0&&m[0].isRenderPass===!0;let T=s.width,E=s.height;for(let D=0;D<m.length;D++){let b=m[D];b.setSize&&b.setSize(T,E)}},this.begin=function(S,T){if(h||S.toneMapping===Fn&&m.length===0)return!1;if(M=T,T!==null){let E=T.width,D=T.height;(s.width!==E||s.height!==D)&&this.setSize(E,D)}return p===!1&&S.setRenderTarget(s),v=S.toneMapping,S.toneMapping=Fn,!0},this.hasRenderPass=function(){return p},this.end=function(S,T){S.toneMapping=v,h=!0;let E=s,D=o;for(let b=0;b<m.length;b++){let A=m[b];if(A.enabled!==!1&&(A.render(S,D,E,T),A.needsSwap!==!1)){let y=E;E=D,D=y}}if(f!==S.outputColorSpace||d!==S.toneMapping){f=S.outputColorSpace,d=S.toneMapping,c.defines={},ze.getTransfer(f)===Qe&&(c.defines.SRGB_TRANSFER="");let b=PI[d];b&&(c.defines[b]=""),c.needsUpdate=!0}c.uniforms.tDiffuse.value=E.texture,S.setRenderTarget(M),S.render(l,u),M=null,h=!1},this.isCompositing=function(){return h},this.dispose=function(){s.depthTexture&&s.depthTexture.dispose(),s.dispose(),o.dispose(),a.dispose(),c.dispose()}}var B_=new xi,yp=new yi(1,1),V_=new Oo,H_=new cl,z_=new Go,x_=[],M_=[],S_=new Float32Array(16),E_=new Float32Array(9),b_=new Float32Array(4);function Us(n,e,t){let i=n[0];if(i<=0||i>0)return n;let r=e*t,s=x_[r];if(s===void 0&&(s=new Float32Array(r),x_[r]=s),e!==0){i.toArray(s,0);for(let o=1,a=0;o!==e;++o)a+=t,n[o].toArray(s,a)}return s}function At(n,e){if(n.length!==e.length)return!1;for(let t=0,i=n.length;t<i;t++)if(n[t]!==e[t])return!1;return!0}function Dt(n,e){for(let t=0,i=e.length;t<i;t++)n[t]=e[t]}function vu(n,e){let t=M_[e];t===void 0&&(t=new Int32Array(e),M_[e]=t);for(let i=0;i!==e;++i)t[i]=n.allocateTextureUnit();return t}function OI(n,e){let t=this.cache;t[0]!==e&&(n.uniform1f(this.addr,e),t[0]=e)}function FI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2f(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(At(t,e))return;n.uniform2fv(this.addr,e),Dt(t,e)}}function kI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3f(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else if(e.r!==void 0)(t[0]!==e.r||t[1]!==e.g||t[2]!==e.b)&&(n.uniform3f(this.addr,e.r,e.g,e.b),t[0]=e.r,t[1]=e.g,t[2]=e.b);else{if(At(t,e))return;n.uniform3fv(this.addr,e),Dt(t,e)}}function UI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4f(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(At(t,e))return;n.uniform4fv(this.addr,e),Dt(t,e)}}function BI(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(At(t,e))return;n.uniformMatrix2fv(this.addr,!1,e),Dt(t,e)}else{if(At(t,i))return;b_.set(i),n.uniformMatrix2fv(this.addr,!1,b_),Dt(t,i)}}function VI(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(At(t,e))return;n.uniformMatrix3fv(this.addr,!1,e),Dt(t,e)}else{if(At(t,i))return;E_.set(i),n.uniformMatrix3fv(this.addr,!1,E_),Dt(t,i)}}function HI(n,e){let t=this.cache,i=e.elements;if(i===void 0){if(At(t,e))return;n.uniformMatrix4fv(this.addr,!1,e),Dt(t,e)}else{if(At(t,i))return;S_.set(i),n.uniformMatrix4fv(this.addr,!1,S_),Dt(t,i)}}function zI(n,e){let t=this.cache;t[0]!==e&&(n.uniform1i(this.addr,e),t[0]=e)}function GI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2i(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(At(t,e))return;n.uniform2iv(this.addr,e),Dt(t,e)}}function WI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3i(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(At(t,e))return;n.uniform3iv(this.addr,e),Dt(t,e)}}function jI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4i(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(At(t,e))return;n.uniform4iv(this.addr,e),Dt(t,e)}}function $I(n,e){let t=this.cache;t[0]!==e&&(n.uniform1ui(this.addr,e),t[0]=e)}function qI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y)&&(n.uniform2ui(this.addr,e.x,e.y),t[0]=e.x,t[1]=e.y);else{if(At(t,e))return;n.uniform2uiv(this.addr,e),Dt(t,e)}}function XI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z)&&(n.uniform3ui(this.addr,e.x,e.y,e.z),t[0]=e.x,t[1]=e.y,t[2]=e.z);else{if(At(t,e))return;n.uniform3uiv(this.addr,e),Dt(t,e)}}function YI(n,e){let t=this.cache;if(e.x!==void 0)(t[0]!==e.x||t[1]!==e.y||t[2]!==e.z||t[3]!==e.w)&&(n.uniform4ui(this.addr,e.x,e.y,e.z,e.w),t[0]=e.x,t[1]=e.y,t[2]=e.z,t[3]=e.w);else{if(At(t,e))return;n.uniform4uiv(this.addr,e),Dt(t,e)}}function ZI(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r);let s;this.type===n.SAMPLER_2D_SHADOW?(yp.compareFunction=t.isReversedDepthBuffer()?uu:lu,s=yp):s=B_,t.setTexture2D(e||s,r)}function JI(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture3D(e||H_,r)}function KI(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTextureCube(e||z_,r)}function QI(n,e,t){let i=this.cache,r=t.allocateTextureUnit();i[0]!==r&&(n.uniform1i(this.addr,r),i[0]=r),t.setTexture2DArray(e||V_,r)}function eC(n){switch(n){case 5126:return OI;case 35664:return FI;case 35665:return kI;case 35666:return UI;case 35674:return BI;case 35675:return VI;case 35676:return HI;case 5124:case 35670:return zI;case 35667:case 35671:return GI;case 35668:case 35672:return WI;case 35669:case 35673:return jI;case 5125:return $I;case 36294:return qI;case 36295:return XI;case 36296:return YI;case 35678:case 36198:case 36298:case 36306:case 35682:return ZI;case 35679:case 36299:case 36307:return JI;case 35680:case 36300:case 36308:case 36293:return KI;case 36289:case 36303:case 36311:case 36292:return QI}}function tC(n,e){n.uniform1fv(this.addr,e)}function nC(n,e){let t=Us(e,this.size,2);n.uniform2fv(this.addr,t)}function iC(n,e){let t=Us(e,this.size,3);n.uniform3fv(this.addr,t)}function rC(n,e){let t=Us(e,this.size,4);n.uniform4fv(this.addr,t)}function sC(n,e){let t=Us(e,this.size,4);n.uniformMatrix2fv(this.addr,!1,t)}function oC(n,e){let t=Us(e,this.size,9);n.uniformMatrix3fv(this.addr,!1,t)}function aC(n,e){let t=Us(e,this.size,16);n.uniformMatrix4fv(this.addr,!1,t)}function cC(n,e){n.uniform1iv(this.addr,e)}function lC(n,e){n.uniform2iv(this.addr,e)}function uC(n,e){n.uniform3iv(this.addr,e)}function dC(n,e){n.uniform4iv(this.addr,e)}function fC(n,e){n.uniform1uiv(this.addr,e)}function hC(n,e){n.uniform2uiv(this.addr,e)}function pC(n,e){n.uniform3uiv(this.addr,e)}function mC(n,e){n.uniform4uiv(this.addr,e)}function gC(n,e,t){let i=this.cache,r=e.length,s=vu(t,r);At(i,s)||(n.uniform1iv(this.addr,s),Dt(i,s));let o;this.type===n.SAMPLER_2D_SHADOW?o=yp:o=B_;for(let a=0;a!==r;++a)t.setTexture2D(e[a]||o,s[a])}function vC(n,e,t){let i=this.cache,r=e.length,s=vu(t,r);At(i,s)||(n.uniform1iv(this.addr,s),Dt(i,s));for(let o=0;o!==r;++o)t.setTexture3D(e[o]||H_,s[o])}function yC(n,e,t){let i=this.cache,r=e.length,s=vu(t,r);At(i,s)||(n.uniform1iv(this.addr,s),Dt(i,s));for(let o=0;o!==r;++o)t.setTextureCube(e[o]||z_,s[o])}function _C(n,e,t){let i=this.cache,r=e.length,s=vu(t,r);At(i,s)||(n.uniform1iv(this.addr,s),Dt(i,s));for(let o=0;o!==r;++o)t.setTexture2DArray(e[o]||V_,s[o])}function xC(n){switch(n){case 5126:return tC;case 35664:return nC;case 35665:return iC;case 35666:return rC;case 35674:return sC;case 35675:return oC;case 35676:return aC;case 5124:case 35670:return cC;case 35667:case 35671:return lC;case 35668:case 35672:return uC;case 35669:case 35673:return dC;case 5125:return fC;case 36294:return hC;case 36295:return pC;case 36296:return mC;case 35678:case 36198:case 36298:case 36306:case 35682:return gC;case 35679:case 36299:case 36307:return vC;case 35680:case 36300:case 36308:case 36293:return yC;case 36289:case 36303:case 36311:case 36292:return _C}}var _p=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.setValue=eC(t.type)}},xp=class{constructor(e,t,i){this.id=e,this.addr=i,this.cache=[],this.type=t.type,this.size=t.size,this.setValue=xC(t.type)}},Mp=class{constructor(e){this.id=e,this.seq=[],this.map={}}setValue(e,t,i){let r=this.seq;for(let s=0,o=r.length;s!==o;++s){let a=r[s];a.setValue(e,t[a.id],i)}}},gp=/(\w+)(\])?(\[|\.)?/g;function T_(n,e){n.seq.push(e),n.map[e.id]=e}function MC(n,e,t){let i=n.name,r=i.length;for(gp.lastIndex=0;;){let s=gp.exec(i),o=gp.lastIndex,a=s[1],c=s[2]==="]",l=s[3];if(c&&(a=a|0),l===void 0||l==="["&&o+2===r){T_(t,l===void 0?new _p(a,n,e):new xp(a,n,e));break}else{let f=t.map[a];f===void 0&&(f=new Mp(a),T_(t,f)),t=f}}}var ks=class{constructor(e,t){this.seq=[],this.map={};let i=e.getProgramParameter(t,e.ACTIVE_UNIFORMS);for(let o=0;o<i;++o){let a=e.getActiveUniform(t,o),c=e.getUniformLocation(t,a.name);MC(a,c,this)}let r=[],s=[];for(let o of this.seq)o.type===e.SAMPLER_2D_SHADOW||o.type===e.SAMPLER_CUBE_SHADOW||o.type===e.SAMPLER_2D_ARRAY_SHADOW?r.push(o):s.push(o);r.length>0&&(this.seq=r.concat(s))}setValue(e,t,i,r){let s=this.map[t];s!==void 0&&s.setValue(e,i,r)}setOptional(e,t,i){let r=t[i];r!==void 0&&this.setValue(e,i,r)}static upload(e,t,i,r){for(let s=0,o=t.length;s!==o;++s){let a=t[s],c=i[a.id];c.needsUpdate!==!1&&a.setValue(e,c.value,r)}}static seqWithValue(e,t){let i=[];for(let r=0,s=e.length;r!==s;++r){let o=e[r];o.id in t&&i.push(o)}return i}};function w_(n,e,t){let i=n.createShader(e);return n.shaderSource(i,t),n.compileShader(i),i}var SC=37297,EC=0;function bC(n,e){let t=n.split(`
`),i=[],r=Math.max(e-6,0),s=Math.min(e+6,t.length);for(let o=r;o<s;o++){let a=o+1;i.push(`${a===e?">":" "} ${a}: ${t[o]}`)}return i.join(`
`)}var I_=new Ae;function TC(n){ze._getMatrix(I_,ze.workingColorSpace,n);let e=`mat3( ${I_.elements.map(t=>t.toFixed(4))} )`;switch(ze.getTransfer(n)){case No:return[e,"LinearTransferOETF"];case Qe:return[e,"sRGBTransferOETF"];default:return be("WebGLProgram: Unsupported color space: ",n),[e,"LinearTransferOETF"]}}function C_(n,e,t){let i=n.getShaderParameter(e,n.COMPILE_STATUS),s=(n.getShaderInfoLog(e)||"").trim();if(i&&s==="")return"";let o=/ERROR: 0:(\d+)/.exec(s);if(o){let a=parseInt(o[1]);return t.toUpperCase()+`

`+s+`

`+bC(n.getShaderSource(e),a)}else return s}function wC(n,e){let t=TC(e);return[`vec4 ${n}( vec4 value ) {`,`	return ${t[1]}( vec4( value.rgb * ${t[0]}, value.a ) );`,"}"].join(`
`)}var IC={[jh]:"Linear",[$h]:"Reinhard",[qh]:"Cineon",[Xh]:"ACESFilmic",[Zh]:"AgX",[Jh]:"Neutral",[Yh]:"Custom"};function CC(n,e){let t=IC[e];return t===void 0?(be("WebGLProgram: Unsupported toneMapping:",e),"vec3 "+n+"( vec3 color ) { return LinearToneMapping( color ); }"):"vec3 "+n+"( vec3 color ) { return "+t+"ToneMapping( color ); }"}var fu=new V;function AC(){ze.getLuminanceCoefficients(fu);let n=fu.x.toFixed(4),e=fu.y.toFixed(4),t=fu.z.toFixed(4);return["float luminance( const in vec3 rgb ) {",`	const vec3 weights = vec3( ${n}, ${e}, ${t} );`,"	return dot( weights, rgb );","}"].join(`
`)}function DC(n){return[n.extensionClipCullDistance?"#extension GL_ANGLE_clip_cull_distance : require":"",n.extensionMultiDraw?"#extension GL_ANGLE_multi_draw : require":""].filter(oa).join(`
`)}function RC(n){let e=[];for(let t in n){let i=n[t];i!==!1&&e.push("#define "+t+" "+i)}return e.join(`
`)}function NC(n,e){let t={},i=n.getProgramParameter(e,n.ACTIVE_ATTRIBUTES);for(let r=0;r<i;r++){let s=n.getActiveAttrib(e,r),o=s.name,a=1;s.type===n.FLOAT_MAT2&&(a=2),s.type===n.FLOAT_MAT3&&(a=3),s.type===n.FLOAT_MAT4&&(a=4),t[o]={type:s.type,location:n.getAttribLocation(e,o),locationSize:a}}return t}function oa(n){return n!==""}function A_(n,e){let t=e.numSpotLightShadows+e.numSpotLightMaps-e.numSpotLightShadowsWithMaps;return n.replace(/NUM_DIR_LIGHTS/g,e.numDirLights).replace(/NUM_SPOT_LIGHTS/g,e.numSpotLights).replace(/NUM_SPOT_LIGHT_MAPS/g,e.numSpotLightMaps).replace(/NUM_SPOT_LIGHT_COORDS/g,t).replace(/NUM_RECT_AREA_LIGHTS/g,e.numRectAreaLights).replace(/NUM_POINT_LIGHTS/g,e.numPointLights).replace(/NUM_HEMI_LIGHTS/g,e.numHemiLights).replace(/NUM_DIR_LIGHT_SHADOWS/g,e.numDirLightShadows).replace(/NUM_SPOT_LIGHT_SHADOWS_WITH_MAPS/g,e.numSpotLightShadowsWithMaps).replace(/NUM_SPOT_LIGHT_SHADOWS/g,e.numSpotLightShadows).replace(/NUM_POINT_LIGHT_SHADOWS/g,e.numPointLightShadows)}function D_(n,e){return n.replace(/NUM_CLIPPING_PLANES/g,e.numClippingPlanes).replace(/UNION_CLIPPING_PLANES/g,e.numClippingPlanes-e.numClipIntersection)}var PC=/^[ \t]*#include +<([\w\d./]+)>/gm;function Sp(n){return n.replace(PC,OC)}var LC=new Map;function OC(n,e){let t=ke[e];if(t===void 0){let i=LC.get(e);if(i!==void 0)t=ke[i],be('WebGLRenderer: Shader chunk "%s" has been deprecated. Use "%s" instead.',e,i);else throw new Error("Can not resolve #include <"+e+">")}return Sp(t)}var FC=/#pragma unroll_loop_start\s+for\s*\(\s*int\s+i\s*=\s*(\d+)\s*;\s*i\s*<\s*(\d+)\s*;\s*i\s*\+\+\s*\)\s*{([\s\S]+?)}\s+#pragma unroll_loop_end/g;function R_(n){return n.replace(FC,kC)}function kC(n,e,t,i){let r="";for(let s=parseInt(e);s<parseInt(t);s++)r+=i.replace(/\[\s*i\s*\]/g,"[ "+s+" ]").replace(/UNROLLED_LOOP_INDEX/g,s);return r}function N_(n){let e=`precision ${n.precision} float;
	precision ${n.precision} int;
	precision ${n.precision} sampler2D;
	precision ${n.precision} samplerCube;
	precision ${n.precision} sampler3D;
	precision ${n.precision} sampler2DArray;
	precision ${n.precision} sampler2DShadow;
	precision ${n.precision} samplerCubeShadow;
	precision ${n.precision} sampler2DArrayShadow;
	precision ${n.precision} isampler2D;
	precision ${n.precision} isampler3D;
	precision ${n.precision} isamplerCube;
	precision ${n.precision} isampler2DArray;
	precision ${n.precision} usampler2D;
	precision ${n.precision} usampler3D;
	precision ${n.precision} usamplerCube;
	precision ${n.precision} usampler2DArray;
	`;return n.precision==="highp"?e+=`
#define HIGH_PRECISION`:n.precision==="mediump"?e+=`
#define MEDIUM_PRECISION`:n.precision==="lowp"&&(e+=`
#define LOW_PRECISION`),e}var UC={[Xo]:"SHADOWMAP_TYPE_PCF",[Ps]:"SHADOWMAP_TYPE_VSM"};function BC(n){return UC[n.shadowMapType]||"SHADOWMAP_TYPE_BASIC"}var VC={[ji]:"ENVMAP_TYPE_CUBE",[Dr]:"ENVMAP_TYPE_CUBE",[Yo]:"ENVMAP_TYPE_CUBE_UV"};function HC(n){return n.envMap===!1?"ENVMAP_TYPE_CUBE":VC[n.envMapMode]||"ENVMAP_TYPE_CUBE"}var zC={[Dr]:"ENVMAP_MODE_REFRACTION"};function GC(n){return n.envMap===!1?"ENVMAP_MODE_REFLECTION":zC[n.envMapMode]||"ENVMAP_MODE_REFLECTION"}var WC={[Wh]:"ENVMAP_BLENDING_MULTIPLY",[Ky]:"ENVMAP_BLENDING_MIX",[Qy]:"ENVMAP_BLENDING_ADD"};function jC(n){return n.envMap===!1?"ENVMAP_BLENDING_NONE":WC[n.combine]||"ENVMAP_BLENDING_NONE"}function $C(n){let e=n.envMapCubeUVHeight;if(e===null)return null;let t=Math.log2(e)-2,i=1/e;return{texelWidth:1/(3*Math.max(Math.pow(2,t),112)),texelHeight:i,maxMip:t}}function qC(n,e,t,i){let r=n.getContext(),s=t.defines,o=t.vertexShader,a=t.fragmentShader,c=BC(t),l=HC(t),u=GC(t),f=jC(t),d=$C(t),h=DC(t),v=RC(s),M=r.createProgram(),m,p,S=t.glslVersion?"#version "+t.glslVersion+`
`:"";t.isRawShaderMaterial?(m=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,v].filter(oa).join(`
`),m.length>0&&(m+=`
`),p=["#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,v].filter(oa).join(`
`),p.length>0&&(p+=`
`)):(m=[N_(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,v,t.extensionClipCullDistance?"#define USE_CLIP_DISTANCE":"",t.batching?"#define USE_BATCHING":"",t.batchingColor?"#define USE_BATCHING_COLOR":"",t.instancing?"#define USE_INSTANCING":"",t.instancingColor?"#define USE_INSTANCING_COLOR":"",t.instancingMorph?"#define USE_INSTANCING_MORPH":"",t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.map?"#define USE_MAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+u:"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.displacementMap?"#define USE_DISPLACEMENTMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.mapUv?"#define MAP_UV "+t.mapUv:"",t.alphaMapUv?"#define ALPHAMAP_UV "+t.alphaMapUv:"",t.lightMapUv?"#define LIGHTMAP_UV "+t.lightMapUv:"",t.aoMapUv?"#define AOMAP_UV "+t.aoMapUv:"",t.emissiveMapUv?"#define EMISSIVEMAP_UV "+t.emissiveMapUv:"",t.bumpMapUv?"#define BUMPMAP_UV "+t.bumpMapUv:"",t.normalMapUv?"#define NORMALMAP_UV "+t.normalMapUv:"",t.displacementMapUv?"#define DISPLACEMENTMAP_UV "+t.displacementMapUv:"",t.metalnessMapUv?"#define METALNESSMAP_UV "+t.metalnessMapUv:"",t.roughnessMapUv?"#define ROUGHNESSMAP_UV "+t.roughnessMapUv:"",t.anisotropyMapUv?"#define ANISOTROPYMAP_UV "+t.anisotropyMapUv:"",t.clearcoatMapUv?"#define CLEARCOATMAP_UV "+t.clearcoatMapUv:"",t.clearcoatNormalMapUv?"#define CLEARCOAT_NORMALMAP_UV "+t.clearcoatNormalMapUv:"",t.clearcoatRoughnessMapUv?"#define CLEARCOAT_ROUGHNESSMAP_UV "+t.clearcoatRoughnessMapUv:"",t.iridescenceMapUv?"#define IRIDESCENCEMAP_UV "+t.iridescenceMapUv:"",t.iridescenceThicknessMapUv?"#define IRIDESCENCE_THICKNESSMAP_UV "+t.iridescenceThicknessMapUv:"",t.sheenColorMapUv?"#define SHEEN_COLORMAP_UV "+t.sheenColorMapUv:"",t.sheenRoughnessMapUv?"#define SHEEN_ROUGHNESSMAP_UV "+t.sheenRoughnessMapUv:"",t.specularMapUv?"#define SPECULARMAP_UV "+t.specularMapUv:"",t.specularColorMapUv?"#define SPECULAR_COLORMAP_UV "+t.specularColorMapUv:"",t.specularIntensityMapUv?"#define SPECULAR_INTENSITYMAP_UV "+t.specularIntensityMapUv:"",t.transmissionMapUv?"#define TRANSMISSIONMAP_UV "+t.transmissionMapUv:"",t.thicknessMapUv?"#define THICKNESSMAP_UV "+t.thicknessMapUv:"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexNormals?"#define HAS_NORMAL":"",t.vertexColors?"#define USE_COLOR":"",t.vertexAlphas?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.flatShading?"#define FLAT_SHADED":"",t.skinning?"#define USE_SKINNING":"",t.morphTargets?"#define USE_MORPHTARGETS":"",t.morphNormals&&t.flatShading===!1?"#define USE_MORPHNORMALS":"",t.morphColors?"#define USE_MORPHCOLORS":"",t.morphTargetsCount>0?"#define MORPHTARGETS_TEXTURE_STRIDE "+t.morphTextureStride:"",t.morphTargetsCount>0?"#define MORPHTARGETS_COUNT "+t.morphTargetsCount:"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.sizeAttenuation?"#define USE_SIZEATTENUATION":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 modelMatrix;","uniform mat4 modelViewMatrix;","uniform mat4 projectionMatrix;","uniform mat4 viewMatrix;","uniform mat3 normalMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;","#ifdef USE_INSTANCING","	attribute mat4 instanceMatrix;","#endif","#ifdef USE_INSTANCING_COLOR","	attribute vec3 instanceColor;","#endif","#ifdef USE_INSTANCING_MORPH","	uniform sampler2D morphTexture;","#endif","attribute vec3 position;","attribute vec3 normal;","attribute vec2 uv;","#ifdef USE_UV1","	attribute vec2 uv1;","#endif","#ifdef USE_UV2","	attribute vec2 uv2;","#endif","#ifdef USE_UV3","	attribute vec2 uv3;","#endif","#ifdef USE_TANGENT","	attribute vec4 tangent;","#endif","#if defined( USE_COLOR_ALPHA )","	attribute vec4 color;","#elif defined( USE_COLOR )","	attribute vec3 color;","#endif","#ifdef USE_SKINNING","	attribute vec4 skinIndex;","	attribute vec4 skinWeight;","#endif",`
`].filter(oa).join(`
`),p=[N_(t),"#define SHADER_TYPE "+t.shaderType,"#define SHADER_NAME "+t.shaderName,v,t.useFog&&t.fog?"#define USE_FOG":"",t.useFog&&t.fogExp2?"#define FOG_EXP2":"",t.alphaToCoverage?"#define ALPHA_TO_COVERAGE":"",t.map?"#define USE_MAP":"",t.matcap?"#define USE_MATCAP":"",t.envMap?"#define USE_ENVMAP":"",t.envMap?"#define "+l:"",t.envMap?"#define "+u:"",t.envMap?"#define "+f:"",d?"#define CUBEUV_TEXEL_WIDTH "+d.texelWidth:"",d?"#define CUBEUV_TEXEL_HEIGHT "+d.texelHeight:"",d?"#define CUBEUV_MAX_MIP "+d.maxMip+".0":"",t.lightMap?"#define USE_LIGHTMAP":"",t.aoMap?"#define USE_AOMAP":"",t.bumpMap?"#define USE_BUMPMAP":"",t.normalMap?"#define USE_NORMALMAP":"",t.normalMapObjectSpace?"#define USE_NORMALMAP_OBJECTSPACE":"",t.normalMapTangentSpace?"#define USE_NORMALMAP_TANGENTSPACE":"",t.packedNormalMap?"#define USE_PACKED_NORMALMAP":"",t.emissiveMap?"#define USE_EMISSIVEMAP":"",t.anisotropy?"#define USE_ANISOTROPY":"",t.anisotropyMap?"#define USE_ANISOTROPYMAP":"",t.clearcoat?"#define USE_CLEARCOAT":"",t.clearcoatMap?"#define USE_CLEARCOATMAP":"",t.clearcoatRoughnessMap?"#define USE_CLEARCOAT_ROUGHNESSMAP":"",t.clearcoatNormalMap?"#define USE_CLEARCOAT_NORMALMAP":"",t.dispersion?"#define USE_DISPERSION":"",t.iridescence?"#define USE_IRIDESCENCE":"",t.iridescenceMap?"#define USE_IRIDESCENCEMAP":"",t.iridescenceThicknessMap?"#define USE_IRIDESCENCE_THICKNESSMAP":"",t.specularMap?"#define USE_SPECULARMAP":"",t.specularColorMap?"#define USE_SPECULAR_COLORMAP":"",t.specularIntensityMap?"#define USE_SPECULAR_INTENSITYMAP":"",t.roughnessMap?"#define USE_ROUGHNESSMAP":"",t.metalnessMap?"#define USE_METALNESSMAP":"",t.alphaMap?"#define USE_ALPHAMAP":"",t.alphaTest?"#define USE_ALPHATEST":"",t.alphaHash?"#define USE_ALPHAHASH":"",t.sheen?"#define USE_SHEEN":"",t.sheenColorMap?"#define USE_SHEEN_COLORMAP":"",t.sheenRoughnessMap?"#define USE_SHEEN_ROUGHNESSMAP":"",t.transmission?"#define USE_TRANSMISSION":"",t.transmissionMap?"#define USE_TRANSMISSIONMAP":"",t.thicknessMap?"#define USE_THICKNESSMAP":"",t.vertexTangents&&t.flatShading===!1?"#define USE_TANGENT":"",t.vertexColors||t.instancingColor?"#define USE_COLOR":"",t.vertexAlphas||t.batchingColor?"#define USE_COLOR_ALPHA":"",t.vertexUv1s?"#define USE_UV1":"",t.vertexUv2s?"#define USE_UV2":"",t.vertexUv3s?"#define USE_UV3":"",t.pointsUvs?"#define USE_POINTS_UV":"",t.gradientMap?"#define USE_GRADIENTMAP":"",t.flatShading?"#define FLAT_SHADED":"",t.doubleSided?"#define DOUBLE_SIDED":"",t.flipSided?"#define FLIP_SIDED":"",t.shadowMapEnabled?"#define USE_SHADOWMAP":"",t.shadowMapEnabled?"#define "+c:"",t.premultipliedAlpha?"#define PREMULTIPLIED_ALPHA":"",t.numLightProbes>0?"#define USE_LIGHT_PROBES":"",t.numLightProbeGrids>0?"#define USE_LIGHT_PROBES_GRID":"",t.decodeVideoTexture?"#define DECODE_VIDEO_TEXTURE":"",t.decodeVideoTextureEmissive?"#define DECODE_VIDEO_TEXTURE_EMISSIVE":"",t.logarithmicDepthBuffer?"#define USE_LOGARITHMIC_DEPTH_BUFFER":"",t.reversedDepthBuffer?"#define USE_REVERSED_DEPTH_BUFFER":"","uniform mat4 viewMatrix;","uniform vec3 cameraPosition;","uniform bool isOrthographic;",t.toneMapping!==Fn?"#define TONE_MAPPING":"",t.toneMapping!==Fn?ke.tonemapping_pars_fragment:"",t.toneMapping!==Fn?CC("toneMapping",t.toneMapping):"",t.dithering?"#define DITHERING":"",t.opaque?"#define OPAQUE":"",ke.colorspace_pars_fragment,wC("linearToOutputTexel",t.outputColorSpace),AC(),t.useDepthPacking?"#define DEPTH_PACKING "+t.depthPacking:"",`
`].filter(oa).join(`
`)),o=Sp(o),o=A_(o,t),o=D_(o,t),a=Sp(a),a=A_(a,t),a=D_(a,t),o=R_(o),a=R_(a),t.isRawShaderMaterial!==!0&&(S=`#version 300 es
`,m=[h,"#define attribute in","#define varying out","#define texture2D texture"].join(`
`)+`
`+m,p=["#define varying in",t.glslVersion===op?"":"layout(location = 0) out highp vec4 pc_fragColor;",t.glslVersion===op?"":"#define gl_FragColor pc_fragColor","#define gl_FragDepthEXT gl_FragDepth","#define texture2D texture","#define textureCube texture","#define texture2DProj textureProj","#define texture2DLodEXT textureLod","#define texture2DProjLodEXT textureProjLod","#define textureCubeLodEXT textureLod","#define texture2DGradEXT textureGrad","#define texture2DProjGradEXT textureProjGrad","#define textureCubeGradEXT textureGrad"].join(`
`)+`
`+p);let T=S+m+o,E=S+p+a,D=w_(r,r.VERTEX_SHADER,T),b=w_(r,r.FRAGMENT_SHADER,E);r.attachShader(M,D),r.attachShader(M,b),t.index0AttributeName!==void 0?r.bindAttribLocation(M,0,t.index0AttributeName):t.morphTargets===!0&&r.bindAttribLocation(M,0,"position"),r.linkProgram(M);function A(I){if(n.debug.checkShaderErrors){let k=r.getProgramInfoLog(M)||"",W=r.getShaderInfoLog(D)||"",X=r.getShaderInfoLog(b)||"",P=k.trim(),z=W.trim(),F=X.trim(),K=!0,Q=!0;if(r.getProgramParameter(M,r.LINK_STATUS)===!1)if(K=!1,typeof n.debug.onShaderError=="function")n.debug.onShaderError(r,M,D,b);else{let le=C_(r,D,"vertex"),ye=C_(r,b,"fragment");we("THREE.WebGLProgram: Shader Error "+r.getError()+" - VALIDATE_STATUS "+r.getProgramParameter(M,r.VALIDATE_STATUS)+`

Material Name: `+I.name+`
Material Type: `+I.type+`

Program Info Log: `+P+`
`+le+`
`+ye)}else P!==""?be("WebGLProgram: Program Info Log:",P):(z===""||F==="")&&(Q=!1);Q&&(I.diagnostics={runnable:K,programLog:P,vertexShader:{log:z,prefix:m},fragmentShader:{log:F,prefix:p}})}r.deleteShader(D),r.deleteShader(b),y=new ks(r,M),w=NC(r,M)}let y;this.getUniforms=function(){return y===void 0&&A(this),y};let w;this.getAttributes=function(){return w===void 0&&A(this),w};let L=t.rendererExtensionParallelShaderCompile===!1;return this.isReady=function(){return L===!1&&(L=r.getProgramParameter(M,SC)),L},this.destroy=function(){i.releaseStatesOfProgram(this),r.deleteProgram(M),this.program=void 0},this.type=t.shaderType,this.name=t.shaderName,this.id=EC++,this.cacheKey=e,this.usedTimes=1,this.program=M,this.vertexShader=D,this.fragmentShader=b,this}var XC=0,Ep=class{constructor(){this.shaderCache=new Map,this.materialCache=new Map}update(e){let t=e.vertexShader,i=e.fragmentShader,r=this._getShaderStage(t),s=this._getShaderStage(i),o=this._getShaderCacheForMaterial(e);return o.has(r)===!1&&(o.add(r),r.usedTimes++),o.has(s)===!1&&(o.add(s),s.usedTimes++),this}remove(e){let t=this.materialCache.get(e);for(let i of t)i.usedTimes--,i.usedTimes===0&&this.shaderCache.delete(i.code);return this.materialCache.delete(e),this}getVertexShaderID(e){return this._getShaderStage(e.vertexShader).id}getFragmentShaderID(e){return this._getShaderStage(e.fragmentShader).id}dispose(){this.shaderCache.clear(),this.materialCache.clear()}_getShaderCacheForMaterial(e){let t=this.materialCache,i=t.get(e);return i===void 0&&(i=new Set,t.set(e,i)),i}_getShaderStage(e){let t=this.shaderCache,i=t.get(e);return i===void 0&&(i=new bp(e),t.set(e,i)),i}},bp=class{constructor(e){this.id=XC++,this.code=e,this.usedTimes=0}};function YC(n){return n===Xi||n===ta||n===na}function ZC(n,e,t,i,r,s){let o=new ko,a=new Ep,c=new Set,l=[],u=new Map,f=i.logarithmicDepthBuffer,d=i.precision,h={MeshDepthMaterial:"depth",MeshDistanceMaterial:"distance",MeshNormalMaterial:"normal",MeshBasicMaterial:"basic",MeshLambertMaterial:"lambert",MeshPhongMaterial:"phong",MeshToonMaterial:"toon",MeshStandardMaterial:"physical",MeshPhysicalMaterial:"physical",MeshMatcapMaterial:"matcap",LineBasicMaterial:"basic",LineDashedMaterial:"dashed",PointsMaterial:"points",ShadowMaterial:"shadow",SpriteMaterial:"sprite"};function v(y){return c.add(y),y===0?"uv":`uv${y}`}function M(y,w,L,I,k,W){let X=I.fog,P=k.geometry,z=y.isMeshStandardMaterial||y.isMeshLambertMaterial||y.isMeshPhongMaterial?I.environment:null,F=y.isMeshStandardMaterial||y.isMeshLambertMaterial&&!y.envMap||y.isMeshPhongMaterial&&!y.envMap,K=e.get(y.envMap||z,F),Q=K&&K.mapping===Yo?K.image.height:null,le=h[y.type];y.precision!==null&&(d=i.getMaxPrecision(y.precision),d!==y.precision&&be("WebGLProgram.getParameters:",y.precision,"not supported, using",d,"instead."));let ye=P.morphAttributes.position||P.morphAttributes.normal||P.morphAttributes.color,Se=ye!==void 0?ye.length:0,qe=0;P.morphAttributes.position!==void 0&&(qe=1),P.morphAttributes.normal!==void 0&&(qe=2),P.morphAttributes.color!==void 0&&(qe=3);let nt,Oe,Y,fe;if(le){let Ne=ni[le];nt=Ne.vertexShader,Oe=Ne.fragmentShader}else nt=y.vertexShader,Oe=y.fragmentShader,a.update(y),Y=a.getVertexShaderID(y),fe=a.getFragmentShaderID(y);let ie=n.getRenderTarget(),Te=n.state.buffers.depth.getReversed(),De=k.isInstancedMesh===!0,Ie=k.isBatchedMesh===!0,pt=!!y.map,Ve=!!y.matcap,it=!!K,ht=!!y.aoMap,Be=!!y.lightMap,wt=!!y.bumpMap,mt=!!y.normalMap,en=!!y.displacementMap,R=!!y.emissiveMap,It=!!y.metalnessMap,He=!!y.roughnessMap,ut=y.anisotropy>0,ae=y.clearcoat>0,gt=y.dispersion>0,x=y.iridescence>0,g=y.sheen>0,O=y.transmission>0,$=ut&&!!y.anisotropyMap,J=ae&&!!y.clearcoatMap,ee=ae&&!!y.clearcoatNormalMap,oe=ae&&!!y.clearcoatRoughnessMap,G=x&&!!y.iridescenceMap,q=x&&!!y.iridescenceThicknessMap,he=g&&!!y.sheenColorMap,ge=g&&!!y.sheenRoughnessMap,re=!!y.specularMap,te=!!y.specularColorMap,Ce=!!y.specularIntensityMap,Fe=O&&!!y.transmissionMap,Je=O&&!!y.thicknessMap,C=!!y.gradientMap,ne=!!y.alphaMap,j=y.alphaTest>0,pe=!!y.alphaHash,se=!!y.extensions,Z=Fn;y.toneMapped&&(ie===null||ie.isXRRenderTarget===!0)&&(Z=n.toneMapping);let xe={shaderID:le,shaderType:y.type,shaderName:y.name,vertexShader:nt,fragmentShader:Oe,defines:y.defines,customVertexShaderID:Y,customFragmentShaderID:fe,isRawShaderMaterial:y.isRawShaderMaterial===!0,glslVersion:y.glslVersion,precision:d,batching:Ie,batchingColor:Ie&&k._colorsTexture!==null,instancing:De,instancingColor:De&&k.instanceColor!==null,instancingMorph:De&&k.morphTexture!==null,outputColorSpace:ie===null?n.outputColorSpace:ie.isXRRenderTarget===!0?ie.texture.colorSpace:ze.workingColorSpace,alphaToCoverage:!!y.alphaToCoverage,map:pt,matcap:Ve,envMap:it,envMapMode:it&&K.mapping,envMapCubeUVHeight:Q,aoMap:ht,lightMap:Be,bumpMap:wt,normalMap:mt,displacementMap:en,emissiveMap:R,normalMapObjectSpace:mt&&y.normalMapType===n_,normalMapTangentSpace:mt&&y.normalMapType===sp,packedNormalMap:mt&&y.normalMapType===sp&&YC(y.normalMap.format),metalnessMap:It,roughnessMap:He,anisotropy:ut,anisotropyMap:$,clearcoat:ae,clearcoatMap:J,clearcoatNormalMap:ee,clearcoatRoughnessMap:oe,dispersion:gt,iridescence:x,iridescenceMap:G,iridescenceThicknessMap:q,sheen:g,sheenColorMap:he,sheenRoughnessMap:ge,specularMap:re,specularColorMap:te,specularIntensityMap:Ce,transmission:O,transmissionMap:Fe,thicknessMap:Je,gradientMap:C,opaque:y.transparent===!1&&y.blending===Ir&&y.alphaToCoverage===!1,alphaMap:ne,alphaTest:j,alphaHash:pe,combine:y.combine,mapUv:pt&&v(y.map.channel),aoMapUv:ht&&v(y.aoMap.channel),lightMapUv:Be&&v(y.lightMap.channel),bumpMapUv:wt&&v(y.bumpMap.channel),normalMapUv:mt&&v(y.normalMap.channel),displacementMapUv:en&&v(y.displacementMap.channel),emissiveMapUv:R&&v(y.emissiveMap.channel),metalnessMapUv:It&&v(y.metalnessMap.channel),roughnessMapUv:He&&v(y.roughnessMap.channel),anisotropyMapUv:$&&v(y.anisotropyMap.channel),clearcoatMapUv:J&&v(y.clearcoatMap.channel),clearcoatNormalMapUv:ee&&v(y.clearcoatNormalMap.channel),clearcoatRoughnessMapUv:oe&&v(y.clearcoatRoughnessMap.channel),iridescenceMapUv:G&&v(y.iridescenceMap.channel),iridescenceThicknessMapUv:q&&v(y.iridescenceThicknessMap.channel),sheenColorMapUv:he&&v(y.sheenColorMap.channel),sheenRoughnessMapUv:ge&&v(y.sheenRoughnessMap.channel),specularMapUv:re&&v(y.specularMap.channel),specularColorMapUv:te&&v(y.specularColorMap.channel),specularIntensityMapUv:Ce&&v(y.specularIntensityMap.channel),transmissionMapUv:Fe&&v(y.transmissionMap.channel),thicknessMapUv:Je&&v(y.thicknessMap.channel),alphaMapUv:ne&&v(y.alphaMap.channel),vertexTangents:!!P.attributes.tangent&&(mt||ut),vertexNormals:!!P.attributes.normal,vertexColors:y.vertexColors,vertexAlphas:y.vertexColors===!0&&!!P.attributes.color&&P.attributes.color.itemSize===4,pointsUvs:k.isPoints===!0&&!!P.attributes.uv&&(pt||ne),fog:!!X,useFog:y.fog===!0,fogExp2:!!X&&X.isFogExp2,flatShading:y.wireframe===!1&&(y.flatShading===!0||P.attributes.normal===void 0&&mt===!1&&(y.isMeshLambertMaterial||y.isMeshPhongMaterial||y.isMeshStandardMaterial||y.isMeshPhysicalMaterial)),sizeAttenuation:y.sizeAttenuation===!0,logarithmicDepthBuffer:f,reversedDepthBuffer:Te,skinning:k.isSkinnedMesh===!0,morphTargets:P.morphAttributes.position!==void 0,morphNormals:P.morphAttributes.normal!==void 0,morphColors:P.morphAttributes.color!==void 0,morphTargetsCount:Se,morphTextureStride:qe,numDirLights:w.directional.length,numPointLights:w.point.length,numSpotLights:w.spot.length,numSpotLightMaps:w.spotLightMap.length,numRectAreaLights:w.rectArea.length,numHemiLights:w.hemi.length,numDirLightShadows:w.directionalShadowMap.length,numPointLightShadows:w.pointShadowMap.length,numSpotLightShadows:w.spotShadowMap.length,numSpotLightShadowsWithMaps:w.numSpotLightShadowsWithMaps,numLightProbes:w.numLightProbes,numLightProbeGrids:W.length,numClippingPlanes:s.numPlanes,numClipIntersection:s.numIntersection,dithering:y.dithering,shadowMapEnabled:n.shadowMap.enabled&&L.length>0,shadowMapType:n.shadowMap.type,toneMapping:Z,decodeVideoTexture:pt&&y.map.isVideoTexture===!0&&ze.getTransfer(y.map.colorSpace)===Qe,decodeVideoTextureEmissive:R&&y.emissiveMap.isVideoTexture===!0&&ze.getTransfer(y.emissiveMap.colorSpace)===Qe,premultipliedAlpha:y.premultipliedAlpha,doubleSided:y.side===En,flipSided:y.side===qt,useDepthPacking:y.depthPacking>=0,depthPacking:y.depthPacking||0,index0AttributeName:y.index0AttributeName,extensionClipCullDistance:se&&y.extensions.clipCullDistance===!0&&t.has("WEBGL_clip_cull_distance"),extensionMultiDraw:(se&&y.extensions.multiDraw===!0||Ie)&&t.has("WEBGL_multi_draw"),rendererExtensionParallelShaderCompile:t.has("KHR_parallel_shader_compile"),customProgramCacheKey:y.customProgramCacheKey()};return xe.vertexUv1s=c.has(1),xe.vertexUv2s=c.has(2),xe.vertexUv3s=c.has(3),c.clear(),xe}function m(y){let w=[];if(y.shaderID?w.push(y.shaderID):(w.push(y.customVertexShaderID),w.push(y.customFragmentShaderID)),y.defines!==void 0)for(let L in y.defines)w.push(L),w.push(y.defines[L]);return y.isRawShaderMaterial===!1&&(p(w,y),S(w,y),w.push(n.outputColorSpace)),w.push(y.customProgramCacheKey),w.join()}function p(y,w){y.push(w.precision),y.push(w.outputColorSpace),y.push(w.envMapMode),y.push(w.envMapCubeUVHeight),y.push(w.mapUv),y.push(w.alphaMapUv),y.push(w.lightMapUv),y.push(w.aoMapUv),y.push(w.bumpMapUv),y.push(w.normalMapUv),y.push(w.displacementMapUv),y.push(w.emissiveMapUv),y.push(w.metalnessMapUv),y.push(w.roughnessMapUv),y.push(w.anisotropyMapUv),y.push(w.clearcoatMapUv),y.push(w.clearcoatNormalMapUv),y.push(w.clearcoatRoughnessMapUv),y.push(w.iridescenceMapUv),y.push(w.iridescenceThicknessMapUv),y.push(w.sheenColorMapUv),y.push(w.sheenRoughnessMapUv),y.push(w.specularMapUv),y.push(w.specularColorMapUv),y.push(w.specularIntensityMapUv),y.push(w.transmissionMapUv),y.push(w.thicknessMapUv),y.push(w.combine),y.push(w.fogExp2),y.push(w.sizeAttenuation),y.push(w.morphTargetsCount),y.push(w.morphAttributeCount),y.push(w.numDirLights),y.push(w.numPointLights),y.push(w.numSpotLights),y.push(w.numSpotLightMaps),y.push(w.numHemiLights),y.push(w.numRectAreaLights),y.push(w.numDirLightShadows),y.push(w.numPointLightShadows),y.push(w.numSpotLightShadows),y.push(w.numSpotLightShadowsWithMaps),y.push(w.numLightProbes),y.push(w.shadowMapType),y.push(w.toneMapping),y.push(w.numClippingPlanes),y.push(w.numClipIntersection),y.push(w.depthPacking)}function S(y,w){o.disableAll(),w.instancing&&o.enable(0),w.instancingColor&&o.enable(1),w.instancingMorph&&o.enable(2),w.matcap&&o.enable(3),w.envMap&&o.enable(4),w.normalMapObjectSpace&&o.enable(5),w.normalMapTangentSpace&&o.enable(6),w.clearcoat&&o.enable(7),w.iridescence&&o.enable(8),w.alphaTest&&o.enable(9),w.vertexColors&&o.enable(10),w.vertexAlphas&&o.enable(11),w.vertexUv1s&&o.enable(12),w.vertexUv2s&&o.enable(13),w.vertexUv3s&&o.enable(14),w.vertexTangents&&o.enable(15),w.anisotropy&&o.enable(16),w.alphaHash&&o.enable(17),w.batching&&o.enable(18),w.dispersion&&o.enable(19),w.batchingColor&&o.enable(20),w.gradientMap&&o.enable(21),w.packedNormalMap&&o.enable(22),w.vertexNormals&&o.enable(23),y.push(o.mask),o.disableAll(),w.fog&&o.enable(0),w.useFog&&o.enable(1),w.flatShading&&o.enable(2),w.logarithmicDepthBuffer&&o.enable(3),w.reversedDepthBuffer&&o.enable(4),w.skinning&&o.enable(5),w.morphTargets&&o.enable(6),w.morphNormals&&o.enable(7),w.morphColors&&o.enable(8),w.premultipliedAlpha&&o.enable(9),w.shadowMapEnabled&&o.enable(10),w.doubleSided&&o.enable(11),w.flipSided&&o.enable(12),w.useDepthPacking&&o.enable(13),w.dithering&&o.enable(14),w.transmission&&o.enable(15),w.sheen&&o.enable(16),w.opaque&&o.enable(17),w.pointsUvs&&o.enable(18),w.decodeVideoTexture&&o.enable(19),w.decodeVideoTextureEmissive&&o.enable(20),w.alphaToCoverage&&o.enable(21),w.numLightProbeGrids>0&&o.enable(22),y.push(o.mask)}function T(y){let w=h[y.type],L;if(w){let I=ni[w];L=p_.clone(I.uniforms)}else L=y.uniforms;return L}function E(y,w){let L=u.get(w);return L!==void 0?++L.usedTimes:(L=new qC(n,w,y,r),l.push(L),u.set(w,L)),L}function D(y){if(--y.usedTimes===0){let w=l.indexOf(y);l[w]=l[l.length-1],l.pop(),u.delete(y.cacheKey),y.destroy()}}function b(y){a.remove(y)}function A(){a.dispose()}return{getParameters:M,getProgramCacheKey:m,getUniforms:T,acquireProgram:E,releaseProgram:D,releaseShaderCache:b,programs:l,dispose:A}}function JC(){let n=new WeakMap;function e(o){return n.has(o)}function t(o){let a=n.get(o);return a===void 0&&(a={},n.set(o,a)),a}function i(o){n.delete(o)}function r(o,a,c){n.get(o)[a]=c}function s(){n=new WeakMap}return{has:e,get:t,remove:i,update:r,dispose:s}}function KC(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.material.id!==e.material.id?n.material.id-e.material.id:n.materialVariant!==e.materialVariant?n.materialVariant-e.materialVariant:n.z!==e.z?n.z-e.z:n.id-e.id}function P_(n,e){return n.groupOrder!==e.groupOrder?n.groupOrder-e.groupOrder:n.renderOrder!==e.renderOrder?n.renderOrder-e.renderOrder:n.z!==e.z?e.z-n.z:n.id-e.id}function L_(){let n=[],e=0,t=[],i=[],r=[];function s(){e=0,t.length=0,i.length=0,r.length=0}function o(d){let h=0;return d.isInstancedMesh&&(h+=2),d.isSkinnedMesh&&(h+=1),h}function a(d,h,v,M,m,p){let S=n[e];return S===void 0?(S={id:d.id,object:d,geometry:h,material:v,materialVariant:o(d),groupOrder:M,renderOrder:d.renderOrder,z:m,group:p},n[e]=S):(S.id=d.id,S.object=d,S.geometry=h,S.material=v,S.materialVariant=o(d),S.groupOrder=M,S.renderOrder=d.renderOrder,S.z=m,S.group=p),e++,S}function c(d,h,v,M,m,p){let S=a(d,h,v,M,m,p);v.transmission>0?i.push(S):v.transparent===!0?r.push(S):t.push(S)}function l(d,h,v,M,m,p){let S=a(d,h,v,M,m,p);v.transmission>0?i.unshift(S):v.transparent===!0?r.unshift(S):t.unshift(S)}function u(d,h){t.length>1&&t.sort(d||KC),i.length>1&&i.sort(h||P_),r.length>1&&r.sort(h||P_)}function f(){for(let d=e,h=n.length;d<h;d++){let v=n[d];if(v.id===null)break;v.id=null,v.object=null,v.geometry=null,v.material=null,v.group=null}}return{opaque:t,transmissive:i,transparent:r,init:s,push:c,unshift:l,finish:f,sort:u}}function QC(){let n=new WeakMap;function e(i,r){let s=n.get(i),o;return s===void 0?(o=new L_,n.set(i,[o])):r>=s.length?(o=new L_,s.push(o)):o=s[r],o}function t(){n=new WeakMap}return{get:e,dispose:t}}function eA(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={direction:new V,color:new Ze};break;case"SpotLight":t={position:new V,direction:new V,color:new Ze,distance:0,coneCos:0,penumbraCos:0,decay:0};break;case"PointLight":t={position:new V,color:new Ze,distance:0,decay:0};break;case"HemisphereLight":t={direction:new V,skyColor:new Ze,groundColor:new Ze};break;case"RectAreaLight":t={color:new Ze,position:new V,halfWidth:new V,halfHeight:new V};break}return n[e.id]=t,t}}}function tA(){let n={};return{get:function(e){if(n[e.id]!==void 0)return n[e.id];let t;switch(e.type){case"DirectionalLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new tt};break;case"SpotLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new tt};break;case"PointLight":t={shadowIntensity:1,shadowBias:0,shadowNormalBias:0,shadowRadius:1,shadowMapSize:new tt,shadowCameraNear:1,shadowCameraFar:1e3};break}return n[e.id]=t,t}}}var nA=0;function iA(n,e){return(e.castShadow?2:0)-(n.castShadow?2:0)+(e.map?1:0)-(n.map?1:0)}function rA(n){let e=new eA,t=tA(),i={version:0,hash:{directionalLength:-1,pointLength:-1,spotLength:-1,rectAreaLength:-1,hemiLength:-1,numDirectionalShadows:-1,numPointShadows:-1,numSpotShadows:-1,numSpotMaps:-1,numLightProbes:-1},ambient:[0,0,0],probe:[],directional:[],directionalShadow:[],directionalShadowMap:[],directionalShadowMatrix:[],spot:[],spotLightMap:[],spotShadow:[],spotShadowMap:[],spotLightMatrix:[],rectArea:[],rectAreaLTC1:null,rectAreaLTC2:null,point:[],pointShadow:[],pointShadowMap:[],pointShadowMatrix:[],hemi:[],numSpotLightShadowsWithMaps:0,numLightProbes:0};for(let l=0;l<9;l++)i.probe.push(new V);let r=new V,s=new Tt,o=new Tt;function a(l){let u=0,f=0,d=0;for(let w=0;w<9;w++)i.probe[w].set(0,0,0);let h=0,v=0,M=0,m=0,p=0,S=0,T=0,E=0,D=0,b=0,A=0;l.sort(iA);for(let w=0,L=l.length;w<L;w++){let I=l[w],k=I.color,W=I.intensity,X=I.distance,P=null;if(I.shadow&&I.shadow.map&&(I.shadow.map.texture.format===Xi?P=I.shadow.map.texture:P=I.shadow.map.depthTexture||I.shadow.map.texture),I.isAmbientLight)u+=k.r*W,f+=k.g*W,d+=k.b*W;else if(I.isLightProbe){for(let z=0;z<9;z++)i.probe[z].addScaledVector(I.sh.coefficients[z],W);A++}else if(I.isDirectionalLight){let z=e.get(I);if(z.color.copy(I.color).multiplyScalar(I.intensity),I.castShadow){let F=I.shadow,K=t.get(I);K.shadowIntensity=F.intensity,K.shadowBias=F.bias,K.shadowNormalBias=F.normalBias,K.shadowRadius=F.radius,K.shadowMapSize=F.mapSize,i.directionalShadow[h]=K,i.directionalShadowMap[h]=P,i.directionalShadowMatrix[h]=I.shadow.matrix,S++}i.directional[h]=z,h++}else if(I.isSpotLight){let z=e.get(I);z.position.setFromMatrixPosition(I.matrixWorld),z.color.copy(k).multiplyScalar(W),z.distance=X,z.coneCos=Math.cos(I.angle),z.penumbraCos=Math.cos(I.angle*(1-I.penumbra)),z.decay=I.decay,i.spot[M]=z;let F=I.shadow;if(I.map&&(i.spotLightMap[D]=I.map,D++,F.updateMatrices(I),I.castShadow&&b++),i.spotLightMatrix[M]=F.matrix,I.castShadow){let K=t.get(I);K.shadowIntensity=F.intensity,K.shadowBias=F.bias,K.shadowNormalBias=F.normalBias,K.shadowRadius=F.radius,K.shadowMapSize=F.mapSize,i.spotShadow[M]=K,i.spotShadowMap[M]=P,E++}M++}else if(I.isRectAreaLight){let z=e.get(I);z.color.copy(k).multiplyScalar(W),z.halfWidth.set(I.width*.5,0,0),z.halfHeight.set(0,I.height*.5,0),i.rectArea[m]=z,m++}else if(I.isPointLight){let z=e.get(I);if(z.color.copy(I.color).multiplyScalar(I.intensity),z.distance=I.distance,z.decay=I.decay,I.castShadow){let F=I.shadow,K=t.get(I);K.shadowIntensity=F.intensity,K.shadowBias=F.bias,K.shadowNormalBias=F.normalBias,K.shadowRadius=F.radius,K.shadowMapSize=F.mapSize,K.shadowCameraNear=F.camera.near,K.shadowCameraFar=F.camera.far,i.pointShadow[v]=K,i.pointShadowMap[v]=P,i.pointShadowMatrix[v]=I.shadow.matrix,T++}i.point[v]=z,v++}else if(I.isHemisphereLight){let z=e.get(I);z.skyColor.copy(I.color).multiplyScalar(W),z.groundColor.copy(I.groundColor).multiplyScalar(W),i.hemi[p]=z,p++}}m>0&&(n.has("OES_texture_float_linear")===!0?(i.rectAreaLTC1=ce.LTC_FLOAT_1,i.rectAreaLTC2=ce.LTC_FLOAT_2):(i.rectAreaLTC1=ce.LTC_HALF_1,i.rectAreaLTC2=ce.LTC_HALF_2)),i.ambient[0]=u,i.ambient[1]=f,i.ambient[2]=d;let y=i.hash;(y.directionalLength!==h||y.pointLength!==v||y.spotLength!==M||y.rectAreaLength!==m||y.hemiLength!==p||y.numDirectionalShadows!==S||y.numPointShadows!==T||y.numSpotShadows!==E||y.numSpotMaps!==D||y.numLightProbes!==A)&&(i.directional.length=h,i.spot.length=M,i.rectArea.length=m,i.point.length=v,i.hemi.length=p,i.directionalShadow.length=S,i.directionalShadowMap.length=S,i.pointShadow.length=T,i.pointShadowMap.length=T,i.spotShadow.length=E,i.spotShadowMap.length=E,i.directionalShadowMatrix.length=S,i.pointShadowMatrix.length=T,i.spotLightMatrix.length=E+D-b,i.spotLightMap.length=D,i.numSpotLightShadowsWithMaps=b,i.numLightProbes=A,y.directionalLength=h,y.pointLength=v,y.spotLength=M,y.rectAreaLength=m,y.hemiLength=p,y.numDirectionalShadows=S,y.numPointShadows=T,y.numSpotShadows=E,y.numSpotMaps=D,y.numLightProbes=A,i.version=nA++)}function c(l,u){let f=0,d=0,h=0,v=0,M=0,m=u.matrixWorldInverse;for(let p=0,S=l.length;p<S;p++){let T=l[p];if(T.isDirectionalLight){let E=i.directional[f];E.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),f++}else if(T.isSpotLight){let E=i.spot[h];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),E.direction.setFromMatrixPosition(T.matrixWorld),r.setFromMatrixPosition(T.target.matrixWorld),E.direction.sub(r),E.direction.transformDirection(m),h++}else if(T.isRectAreaLight){let E=i.rectArea[v];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),o.identity(),s.copy(T.matrixWorld),s.premultiply(m),o.extractRotation(s),E.halfWidth.set(T.width*.5,0,0),E.halfHeight.set(0,T.height*.5,0),E.halfWidth.applyMatrix4(o),E.halfHeight.applyMatrix4(o),v++}else if(T.isPointLight){let E=i.point[d];E.position.setFromMatrixPosition(T.matrixWorld),E.position.applyMatrix4(m),d++}else if(T.isHemisphereLight){let E=i.hemi[M];E.direction.setFromMatrixPosition(T.matrixWorld),E.direction.transformDirection(m),M++}}}return{setup:a,setupView:c,state:i}}function O_(n){let e=new rA(n),t=[],i=[],r=[];function s(d){f.camera=d,t.length=0,i.length=0,r.length=0}function o(d){t.push(d)}function a(d){i.push(d)}function c(d){r.push(d)}function l(){e.setup(t)}function u(d){e.setupView(t,d)}let f={lightsArray:t,shadowsArray:i,lightProbeGridArray:r,camera:null,lights:e,transmissionRenderTarget:{},textureUnits:0};return{init:s,state:f,setupLights:l,setupLightsView:u,pushLight:o,pushShadow:a,pushLightProbeGrid:c}}function sA(n){let e=new WeakMap;function t(r,s=0){let o=e.get(r),a;return o===void 0?(a=new O_(n),e.set(r,[a])):s>=o.length?(a=new O_(n),o.push(a)):a=o[s],a}function i(){e=new WeakMap}return{get:t,dispose:i}}var oA=`void main() {
	gl_Position = vec4( position, 1.0 );
}`,aA=`uniform sampler2D shadow_pass;
uniform vec2 resolution;
uniform float radius;
void main() {
	const float samples = float( VSM_SAMPLES );
	float mean = 0.0;
	float squared_mean = 0.0;
	float uvStride = samples <= 1.0 ? 0.0 : 2.0 / ( samples - 1.0 );
	float uvStart = samples <= 1.0 ? 0.0 : - 1.0;
	for ( float i = 0.0; i < samples; i ++ ) {
		float uvOffset = uvStart + i * uvStride;
		#ifdef HORIZONTAL_PASS
			vec2 distribution = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( uvOffset, 0.0 ) * radius ) / resolution ).rg;
			mean += distribution.x;
			squared_mean += distribution.y * distribution.y + distribution.x * distribution.x;
		#else
			float depth = texture2D( shadow_pass, ( gl_FragCoord.xy + vec2( 0.0, uvOffset ) * radius ) / resolution ).r;
			mean += depth;
			squared_mean += depth * depth;
		#endif
	}
	mean = mean / samples;
	squared_mean = squared_mean / samples;
	float std_dev = sqrt( max( 0.0, squared_mean - mean * mean ) );
	gl_FragColor = vec4( mean, std_dev, 0.0, 1.0 );
}`,cA=[new V(1,0,0),new V(-1,0,0),new V(0,1,0),new V(0,-1,0),new V(0,0,1),new V(0,0,-1)],lA=[new V(0,-1,0),new V(0,-1,0),new V(0,0,1),new V(0,0,-1),new V(0,-1,0),new V(0,-1,0)],F_=new Tt,sa=new V,vp=new V;function uA(n,e,t){let i=new zo,r=new tt,s=new tt,o=new _t,a=new fl,c=new hl,l={},u=t.maxTextureSize,f={[gi]:qt,[qt]:gi,[En]:En},d=new fn({defines:{VSM_SAMPLES:8},uniforms:{shadow_pass:{value:null},resolution:{value:new tt},radius:{value:4}},vertexShader:oA,fragmentShader:aA}),h=d.clone();h.defines.HORIZONTAL_PASS=1;let v=new Sn;v.setAttribute("position",new $t(new Float32Array([-1,-1,.5,3,-1,.5,-1,3,.5]),3));let M=new Qt(v,d),m=this;this.enabled=!1,this.autoUpdate=!0,this.needsUpdate=!1,this.type=Xo;let p=this.type;this.render=function(b,A,y){if(m.enabled===!1||m.autoUpdate===!1&&m.needsUpdate===!1||b.length===0)return;this.type===Py&&(be("WebGLShadowMap: PCFSoftShadowMap has been deprecated. Using PCFShadowMap instead."),this.type=Xo);let w=n.getRenderTarget(),L=n.getActiveCubeFace(),I=n.getActiveMipmapLevel(),k=n.state;k.setBlending(Qn),k.buffers.depth.getReversed()===!0?k.buffers.color.setClear(0,0,0,0):k.buffers.color.setClear(1,1,1,1),k.buffers.depth.setTest(!0),k.setScissorTest(!1);let W=p!==this.type;W&&A.traverse(function(X){X.material&&(Array.isArray(X.material)?X.material.forEach(P=>P.needsUpdate=!0):X.material.needsUpdate=!0)});for(let X=0,P=b.length;X<P;X++){let z=b[X],F=z.shadow;if(F===void 0){be("WebGLShadowMap:",z,"has no shadow.");continue}if(F.autoUpdate===!1&&F.needsUpdate===!1)continue;r.copy(F.mapSize);let K=F.getFrameExtents();r.multiply(K),s.copy(F.mapSize),(r.x>u||r.y>u)&&(r.x>u&&(s.x=Math.floor(u/K.x),r.x=s.x*K.x,F.mapSize.x=s.x),r.y>u&&(s.y=Math.floor(u/K.y),r.y=s.y*K.y,F.mapSize.y=s.y));let Q=n.state.buffers.depth.getReversed();if(F.camera._reversedDepth=Q,F.map===null||W===!0){if(F.map!==null&&(F.map.depthTexture!==null&&(F.map.depthTexture.dispose(),F.map.depthTexture=null),F.map.dispose()),this.type===Ps){if(z.isPointLight){be("WebGLShadowMap: VSM shadow maps are not supported for PointLights. Use PCF or BasicShadowMap instead.");continue}F.map=new dn(r.x,r.y,{format:Xi,type:ei,minFilter:Ot,magFilter:Ot,generateMipmaps:!1}),F.map.texture.name=z.name+".shadowMap",F.map.depthTexture=new yi(r.x,r.y,Un),F.map.depthTexture.name=z.name+".shadowMapDepth",F.map.depthTexture.format=Zn,F.map.depthTexture.compareFunction=null,F.map.depthTexture.minFilter=Pt,F.map.depthTexture.magFilter=Pt}else z.isPointLight?(F.map=new pu(r.x),F.map.depthTexture=new dl(r.x,kn)):(F.map=new dn(r.x,r.y),F.map.depthTexture=new yi(r.x,r.y,kn)),F.map.depthTexture.name=z.name+".shadowMap",F.map.depthTexture.format=Zn,this.type===Xo?(F.map.depthTexture.compareFunction=Q?uu:lu,F.map.depthTexture.minFilter=Ot,F.map.depthTexture.magFilter=Ot):(F.map.depthTexture.compareFunction=null,F.map.depthTexture.minFilter=Pt,F.map.depthTexture.magFilter=Pt);F.camera.updateProjectionMatrix()}let le=F.map.isWebGLCubeRenderTarget?6:1;for(let ye=0;ye<le;ye++){if(F.map.isWebGLCubeRenderTarget)n.setRenderTarget(F.map,ye),n.clear();else{ye===0&&(n.setRenderTarget(F.map),n.clear());let Se=F.getViewport(ye);o.set(s.x*Se.x,s.y*Se.y,s.x*Se.z,s.y*Se.w),k.viewport(o)}if(z.isPointLight){let Se=F.camera,qe=F.matrix,nt=z.distance||Se.far;nt!==Se.far&&(Se.far=nt,Se.updateProjectionMatrix()),sa.setFromMatrixPosition(z.matrixWorld),Se.position.copy(sa),vp.copy(Se.position),vp.add(cA[ye]),Se.up.copy(lA[ye]),Se.lookAt(vp),Se.updateMatrixWorld(),qe.makeTranslation(-sa.x,-sa.y,-sa.z),F_.multiplyMatrices(Se.projectionMatrix,Se.matrixWorldInverse),F._frustum.setFromProjectionMatrix(F_,Se.coordinateSystem,Se.reversedDepth)}else F.updateMatrices(z);i=F.getFrustum(),E(A,y,F.camera,z,this.type)}F.isPointLightShadow!==!0&&this.type===Ps&&S(F,y),F.needsUpdate=!1}p=this.type,m.needsUpdate=!1,n.setRenderTarget(w,L,I)};function S(b,A){let y=e.update(M);d.defines.VSM_SAMPLES!==b.blurSamples&&(d.defines.VSM_SAMPLES=b.blurSamples,h.defines.VSM_SAMPLES=b.blurSamples,d.needsUpdate=!0,h.needsUpdate=!0),b.mapPass===null&&(b.mapPass=new dn(r.x,r.y,{format:Xi,type:ei})),d.uniforms.shadow_pass.value=b.map.depthTexture,d.uniforms.resolution.value=b.mapSize,d.uniforms.radius.value=b.radius,n.setRenderTarget(b.mapPass),n.clear(),n.renderBufferDirect(A,null,y,d,M,null),h.uniforms.shadow_pass.value=b.mapPass.texture,h.uniforms.resolution.value=b.mapSize,h.uniforms.radius.value=b.radius,n.setRenderTarget(b.map),n.clear(),n.renderBufferDirect(A,null,y,h,M,null)}function T(b,A,y,w){let L=null,I=y.isPointLight===!0?b.customDistanceMaterial:b.customDepthMaterial;if(I!==void 0)L=I;else if(L=y.isPointLight===!0?c:a,n.localClippingEnabled&&A.clipShadows===!0&&Array.isArray(A.clippingPlanes)&&A.clippingPlanes.length!==0||A.displacementMap&&A.displacementScale!==0||A.alphaMap&&A.alphaTest>0||A.map&&A.alphaTest>0||A.alphaToCoverage===!0){let k=L.uuid,W=A.uuid,X=l[k];X===void 0&&(X={},l[k]=X);let P=X[W];P===void 0&&(P=L.clone(),X[W]=P,A.addEventListener("dispose",D)),L=P}if(L.visible=A.visible,L.wireframe=A.wireframe,w===Ps?L.side=A.shadowSide!==null?A.shadowSide:A.side:L.side=A.shadowSide!==null?A.shadowSide:f[A.side],L.alphaMap=A.alphaMap,L.alphaTest=A.alphaToCoverage===!0?.5:A.alphaTest,L.map=A.map,L.clipShadows=A.clipShadows,L.clippingPlanes=A.clippingPlanes,L.clipIntersection=A.clipIntersection,L.displacementMap=A.displacementMap,L.displacementScale=A.displacementScale,L.displacementBias=A.displacementBias,L.wireframeLinewidth=A.wireframeLinewidth,L.linewidth=A.linewidth,y.isPointLight===!0&&L.isMeshDistanceMaterial===!0){let k=n.properties.get(L);k.light=y}return L}function E(b,A,y,w,L){if(b.visible===!1)return;if(b.layers.test(A.layers)&&(b.isMesh||b.isLine||b.isPoints)&&(b.castShadow||b.receiveShadow&&L===Ps)&&(!b.frustumCulled||i.intersectsObject(b))){b.modelViewMatrix.multiplyMatrices(y.matrixWorldInverse,b.matrixWorld);let W=e.update(b),X=b.material;if(Array.isArray(X)){let P=W.groups;for(let z=0,F=P.length;z<F;z++){let K=P[z],Q=X[K.materialIndex];if(Q&&Q.visible){let le=T(b,Q,w,L);b.onBeforeShadow(n,b,A,y,W,le,K),n.renderBufferDirect(y,null,W,le,b,K),b.onAfterShadow(n,b,A,y,W,le,K)}}}else if(X.visible){let P=T(b,X,w,L);b.onBeforeShadow(n,b,A,y,W,P,null),n.renderBufferDirect(y,null,W,P,b,null),b.onAfterShadow(n,b,A,y,W,P,null)}}let k=b.children;for(let W=0,X=k.length;W<X;W++)E(k[W],A,y,w,L)}function D(b){b.target.removeEventListener("dispose",D);for(let y in l){let w=l[y],L=b.target.uuid;L in w&&(w[L].dispose(),delete w[L])}}}function dA(n,e){function t(){let C=!1,ne=new _t,j=null,pe=new _t(0,0,0,0);return{setMask:function(se){j!==se&&!C&&(n.colorMask(se,se,se,se),j=se)},setLocked:function(se){C=se},setClear:function(se,Z,xe,Ne,xt){xt===!0&&(se*=Ne,Z*=Ne,xe*=Ne),ne.set(se,Z,xe,Ne),pe.equals(ne)===!1&&(n.clearColor(se,Z,xe,Ne),pe.copy(ne))},reset:function(){C=!1,j=null,pe.set(-1,0,0,0)}}}function i(){let C=!1,ne=!1,j=null,pe=null,se=null;return{setReversed:function(Z){if(ne!==Z){let xe=e.get("EXT_clip_control");Z?xe.clipControlEXT(xe.LOWER_LEFT_EXT,xe.ZERO_TO_ONE_EXT):xe.clipControlEXT(xe.LOWER_LEFT_EXT,xe.NEGATIVE_ONE_TO_ONE_EXT),ne=Z;let Ne=se;se=null,this.setClear(Ne)}},getReversed:function(){return ne},setTest:function(Z){Z?ie(n.DEPTH_TEST):Te(n.DEPTH_TEST)},setMask:function(Z){j!==Z&&!C&&(n.depthMask(Z),j=Z)},setFunc:function(Z){if(ne&&(Z=f_[Z]),pe!==Z){switch(Z){case Xc:n.depthFunc(n.NEVER);break;case Yc:n.depthFunc(n.ALWAYS);break;case Zc:n.depthFunc(n.LESS);break;case Cr:n.depthFunc(n.LEQUAL);break;case Jc:n.depthFunc(n.EQUAL);break;case Kc:n.depthFunc(n.GEQUAL);break;case Qc:n.depthFunc(n.GREATER);break;case el:n.depthFunc(n.NOTEQUAL);break;default:n.depthFunc(n.LEQUAL)}pe=Z}},setLocked:function(Z){C=Z},setClear:function(Z){se!==Z&&(se=Z,ne&&(Z=1-Z),n.clearDepth(Z))},reset:function(){C=!1,j=null,pe=null,se=null,ne=!1}}}function r(){let C=!1,ne=null,j=null,pe=null,se=null,Z=null,xe=null,Ne=null,xt=null;return{setTest:function(rt){C||(rt?ie(n.STENCIL_TEST):Te(n.STENCIL_TEST))},setMask:function(rt){ne!==rt&&!C&&(n.stencilMask(rt),ne=rt)},setFunc:function(rt,ii,Bn){(j!==rt||pe!==ii||se!==Bn)&&(n.stencilFunc(rt,ii,Bn),j=rt,pe=ii,se=Bn)},setOp:function(rt,ii,Bn){(Z!==rt||xe!==ii||Ne!==Bn)&&(n.stencilOp(rt,ii,Bn),Z=rt,xe=ii,Ne=Bn)},setLocked:function(rt){C=rt},setClear:function(rt){xt!==rt&&(n.clearStencil(rt),xt=rt)},reset:function(){C=!1,ne=null,j=null,pe=null,se=null,Z=null,xe=null,Ne=null,xt=null}}}let s=new t,o=new i,a=new r,c=new WeakMap,l=new WeakMap,u={},f={},d={},h=new WeakMap,v=[],M=null,m=!1,p=null,S=null,T=null,E=null,D=null,b=null,A=null,y=new Ze(0,0,0),w=0,L=!1,I=null,k=null,W=null,X=null,P=null,z=n.getParameter(n.MAX_COMBINED_TEXTURE_IMAGE_UNITS),F=!1,K=0,Q=n.getParameter(n.VERSION);Q.indexOf("WebGL")!==-1?(K=parseFloat(/^WebGL (\d)/.exec(Q)[1]),F=K>=1):Q.indexOf("OpenGL ES")!==-1&&(K=parseFloat(/^OpenGL ES (\d)/.exec(Q)[1]),F=K>=2);let le=null,ye={},Se=n.getParameter(n.SCISSOR_BOX),qe=n.getParameter(n.VIEWPORT),nt=new _t().fromArray(Se),Oe=new _t().fromArray(qe);function Y(C,ne,j,pe){let se=new Uint8Array(4),Z=n.createTexture();n.bindTexture(C,Z),n.texParameteri(C,n.TEXTURE_MIN_FILTER,n.NEAREST),n.texParameteri(C,n.TEXTURE_MAG_FILTER,n.NEAREST);for(let xe=0;xe<j;xe++)C===n.TEXTURE_3D||C===n.TEXTURE_2D_ARRAY?n.texImage3D(ne,0,n.RGBA,1,1,pe,0,n.RGBA,n.UNSIGNED_BYTE,se):n.texImage2D(ne+xe,0,n.RGBA,1,1,0,n.RGBA,n.UNSIGNED_BYTE,se);return Z}let fe={};fe[n.TEXTURE_2D]=Y(n.TEXTURE_2D,n.TEXTURE_2D,1),fe[n.TEXTURE_CUBE_MAP]=Y(n.TEXTURE_CUBE_MAP,n.TEXTURE_CUBE_MAP_POSITIVE_X,6),fe[n.TEXTURE_2D_ARRAY]=Y(n.TEXTURE_2D_ARRAY,n.TEXTURE_2D_ARRAY,1,1),fe[n.TEXTURE_3D]=Y(n.TEXTURE_3D,n.TEXTURE_3D,1,1),s.setClear(0,0,0,1),o.setClear(1),a.setClear(0),ie(n.DEPTH_TEST),o.setFunc(Cr),wt(!1),mt(Vh),ie(n.CULL_FACE),ht(Qn);function ie(C){u[C]!==!0&&(n.enable(C),u[C]=!0)}function Te(C){u[C]!==!1&&(n.disable(C),u[C]=!1)}function De(C,ne){return d[C]!==ne?(n.bindFramebuffer(C,ne),d[C]=ne,C===n.DRAW_FRAMEBUFFER&&(d[n.FRAMEBUFFER]=ne),C===n.FRAMEBUFFER&&(d[n.DRAW_FRAMEBUFFER]=ne),!0):!1}function Ie(C,ne){let j=v,pe=!1;if(C){j=h.get(ne),j===void 0&&(j=[],h.set(ne,j));let se=C.textures;if(j.length!==se.length||j[0]!==n.COLOR_ATTACHMENT0){for(let Z=0,xe=se.length;Z<xe;Z++)j[Z]=n.COLOR_ATTACHMENT0+Z;j.length=se.length,pe=!0}}else j[0]!==n.BACK&&(j[0]=n.BACK,pe=!0);pe&&n.drawBuffers(j)}function pt(C){return M!==C?(n.useProgram(C),M=C,!0):!1}let Ve={[Vi]:n.FUNC_ADD,[Oy]:n.FUNC_SUBTRACT,[Fy]:n.FUNC_REVERSE_SUBTRACT};Ve[ky]=n.MIN,Ve[Uy]=n.MAX;let it={[By]:n.ZERO,[Vy]:n.ONE,[Hy]:n.SRC_COLOR,[$c]:n.SRC_ALPHA,[qy]:n.SRC_ALPHA_SATURATE,[jy]:n.DST_COLOR,[Gy]:n.DST_ALPHA,[zy]:n.ONE_MINUS_SRC_COLOR,[qc]:n.ONE_MINUS_SRC_ALPHA,[$y]:n.ONE_MINUS_DST_COLOR,[Wy]:n.ONE_MINUS_DST_ALPHA,[Xy]:n.CONSTANT_COLOR,[Yy]:n.ONE_MINUS_CONSTANT_COLOR,[Zy]:n.CONSTANT_ALPHA,[Jy]:n.ONE_MINUS_CONSTANT_ALPHA};function ht(C,ne,j,pe,se,Z,xe,Ne,xt,rt){if(C===Qn){m===!0&&(Te(n.BLEND),m=!1);return}if(m===!1&&(ie(n.BLEND),m=!0),C!==Ly){if(C!==p||rt!==L){if((S!==Vi||D!==Vi)&&(n.blendEquation(n.FUNC_ADD),S=Vi,D=Vi),rt)switch(C){case Ir:n.blendFuncSeparate(n.ONE,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Hh:n.blendFunc(n.ONE,n.ONE);break;case zh:n.blendFuncSeparate(n.ZERO,n.ONE_MINUS_SRC_COLOR,n.ZERO,n.ONE);break;case Gh:n.blendFuncSeparate(n.DST_COLOR,n.ONE_MINUS_SRC_ALPHA,n.ZERO,n.ONE);break;default:we("WebGLState: Invalid blending: ",C);break}else switch(C){case Ir:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE_MINUS_SRC_ALPHA,n.ONE,n.ONE_MINUS_SRC_ALPHA);break;case Hh:n.blendFuncSeparate(n.SRC_ALPHA,n.ONE,n.ONE,n.ONE);break;case zh:we("WebGLState: SubtractiveBlending requires material.premultipliedAlpha = true");break;case Gh:we("WebGLState: MultiplyBlending requires material.premultipliedAlpha = true");break;default:we("WebGLState: Invalid blending: ",C);break}T=null,E=null,b=null,A=null,y.set(0,0,0),w=0,p=C,L=rt}return}se=se||ne,Z=Z||j,xe=xe||pe,(ne!==S||se!==D)&&(n.blendEquationSeparate(Ve[ne],Ve[se]),S=ne,D=se),(j!==T||pe!==E||Z!==b||xe!==A)&&(n.blendFuncSeparate(it[j],it[pe],it[Z],it[xe]),T=j,E=pe,b=Z,A=xe),(Ne.equals(y)===!1||xt!==w)&&(n.blendColor(Ne.r,Ne.g,Ne.b,xt),y.copy(Ne),w=xt),p=C,L=!1}function Be(C,ne){C.side===En?Te(n.CULL_FACE):ie(n.CULL_FACE);let j=C.side===qt;ne&&(j=!j),wt(j),C.blending===Ir&&C.transparent===!1?ht(Qn):ht(C.blending,C.blendEquation,C.blendSrc,C.blendDst,C.blendEquationAlpha,C.blendSrcAlpha,C.blendDstAlpha,C.blendColor,C.blendAlpha,C.premultipliedAlpha),o.setFunc(C.depthFunc),o.setTest(C.depthTest),o.setMask(C.depthWrite),s.setMask(C.colorWrite);let pe=C.stencilWrite;a.setTest(pe),pe&&(a.setMask(C.stencilWriteMask),a.setFunc(C.stencilFunc,C.stencilRef,C.stencilFuncMask),a.setOp(C.stencilFail,C.stencilZFail,C.stencilZPass)),R(C.polygonOffset,C.polygonOffsetFactor,C.polygonOffsetUnits),C.alphaToCoverage===!0?ie(n.SAMPLE_ALPHA_TO_COVERAGE):Te(n.SAMPLE_ALPHA_TO_COVERAGE)}function wt(C){I!==C&&(C?n.frontFace(n.CW):n.frontFace(n.CCW),I=C)}function mt(C){C!==Ry?(ie(n.CULL_FACE),C!==k&&(C===Vh?n.cullFace(n.BACK):C===Ny?n.cullFace(n.FRONT):n.cullFace(n.FRONT_AND_BACK))):Te(n.CULL_FACE),k=C}function en(C){C!==W&&(F&&n.lineWidth(C),W=C)}function R(C,ne,j){C?(ie(n.POLYGON_OFFSET_FILL),(X!==ne||P!==j)&&(X=ne,P=j,o.getReversed()&&(ne=-ne),n.polygonOffset(ne,j))):Te(n.POLYGON_OFFSET_FILL)}function It(C){C?ie(n.SCISSOR_TEST):Te(n.SCISSOR_TEST)}function He(C){C===void 0&&(C=n.TEXTURE0+z-1),le!==C&&(n.activeTexture(C),le=C)}function ut(C,ne,j){j===void 0&&(le===null?j=n.TEXTURE0+z-1:j=le);let pe=ye[j];pe===void 0&&(pe={type:void 0,texture:void 0},ye[j]=pe),(pe.type!==C||pe.texture!==ne)&&(le!==j&&(n.activeTexture(j),le=j),n.bindTexture(C,ne||fe[C]),pe.type=C,pe.texture=ne)}function ae(){let C=ye[le];C!==void 0&&C.type!==void 0&&(n.bindTexture(C.type,null),C.type=void 0,C.texture=void 0)}function gt(){try{n.compressedTexImage2D(...arguments)}catch(C){we("WebGLState:",C)}}function x(){try{n.compressedTexImage3D(...arguments)}catch(C){we("WebGLState:",C)}}function g(){try{n.texSubImage2D(...arguments)}catch(C){we("WebGLState:",C)}}function O(){try{n.texSubImage3D(...arguments)}catch(C){we("WebGLState:",C)}}function $(){try{n.compressedTexSubImage2D(...arguments)}catch(C){we("WebGLState:",C)}}function J(){try{n.compressedTexSubImage3D(...arguments)}catch(C){we("WebGLState:",C)}}function ee(){try{n.texStorage2D(...arguments)}catch(C){we("WebGLState:",C)}}function oe(){try{n.texStorage3D(...arguments)}catch(C){we("WebGLState:",C)}}function G(){try{n.texImage2D(...arguments)}catch(C){we("WebGLState:",C)}}function q(){try{n.texImage3D(...arguments)}catch(C){we("WebGLState:",C)}}function he(C){return f[C]!==void 0?f[C]:n.getParameter(C)}function ge(C,ne){f[C]!==ne&&(n.pixelStorei(C,ne),f[C]=ne)}function re(C){nt.equals(C)===!1&&(n.scissor(C.x,C.y,C.z,C.w),nt.copy(C))}function te(C){Oe.equals(C)===!1&&(n.viewport(C.x,C.y,C.z,C.w),Oe.copy(C))}function Ce(C,ne){let j=l.get(ne);j===void 0&&(j=new WeakMap,l.set(ne,j));let pe=j.get(C);pe===void 0&&(pe=n.getUniformBlockIndex(ne,C.name),j.set(C,pe))}function Fe(C,ne){let pe=l.get(ne).get(C);c.get(ne)!==pe&&(n.uniformBlockBinding(ne,pe,C.__bindingPointIndex),c.set(ne,pe))}function Je(){n.disable(n.BLEND),n.disable(n.CULL_FACE),n.disable(n.DEPTH_TEST),n.disable(n.POLYGON_OFFSET_FILL),n.disable(n.SCISSOR_TEST),n.disable(n.STENCIL_TEST),n.disable(n.SAMPLE_ALPHA_TO_COVERAGE),n.blendEquation(n.FUNC_ADD),n.blendFunc(n.ONE,n.ZERO),n.blendFuncSeparate(n.ONE,n.ZERO,n.ONE,n.ZERO),n.blendColor(0,0,0,0),n.colorMask(!0,!0,!0,!0),n.clearColor(0,0,0,0),n.depthMask(!0),n.depthFunc(n.LESS),o.setReversed(!1),n.clearDepth(1),n.stencilMask(4294967295),n.stencilFunc(n.ALWAYS,0,4294967295),n.stencilOp(n.KEEP,n.KEEP,n.KEEP),n.clearStencil(0),n.cullFace(n.BACK),n.frontFace(n.CCW),n.polygonOffset(0,0),n.activeTexture(n.TEXTURE0),n.bindFramebuffer(n.FRAMEBUFFER,null),n.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),n.bindFramebuffer(n.READ_FRAMEBUFFER,null),n.useProgram(null),n.lineWidth(1),n.scissor(0,0,n.canvas.width,n.canvas.height),n.viewport(0,0,n.canvas.width,n.canvas.height),n.pixelStorei(n.PACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_ALIGNMENT,4),n.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,!1),n.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,!1),n.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,n.BROWSER_DEFAULT_WEBGL),n.pixelStorei(n.PACK_ROW_LENGTH,0),n.pixelStorei(n.PACK_SKIP_PIXELS,0),n.pixelStorei(n.PACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_ROW_LENGTH,0),n.pixelStorei(n.UNPACK_IMAGE_HEIGHT,0),n.pixelStorei(n.UNPACK_SKIP_PIXELS,0),n.pixelStorei(n.UNPACK_SKIP_ROWS,0),n.pixelStorei(n.UNPACK_SKIP_IMAGES,0),u={},f={},le=null,ye={},d={},h=new WeakMap,v=[],M=null,m=!1,p=null,S=null,T=null,E=null,D=null,b=null,A=null,y=new Ze(0,0,0),w=0,L=!1,I=null,k=null,W=null,X=null,P=null,nt.set(0,0,n.canvas.width,n.canvas.height),Oe.set(0,0,n.canvas.width,n.canvas.height),s.reset(),o.reset(),a.reset()}return{buffers:{color:s,depth:o,stencil:a},enable:ie,disable:Te,bindFramebuffer:De,drawBuffers:Ie,useProgram:pt,setBlending:ht,setMaterial:Be,setFlipSided:wt,setCullFace:mt,setLineWidth:en,setPolygonOffset:R,setScissorTest:It,activeTexture:He,bindTexture:ut,unbindTexture:ae,compressedTexImage2D:gt,compressedTexImage3D:x,texImage2D:G,texImage3D:q,pixelStorei:ge,getParameter:he,updateUBOMapping:Ce,uniformBlockBinding:Fe,texStorage2D:ee,texStorage3D:oe,texSubImage2D:g,texSubImage3D:O,compressedTexSubImage2D:$,compressedTexSubImage3D:J,scissor:re,viewport:te,reset:Je}}function fA(n,e,t,i,r,s,o){let a=e.has("WEBGL_multisampled_render_to_texture")?e.get("WEBGL_multisampled_render_to_texture"):null,c=typeof navigator>"u"?!1:/OculusBrowser/g.test(navigator.userAgent),l=new tt,u=new WeakMap,f=new Set,d,h=new WeakMap,v=!1;try{v=typeof OffscreenCanvas<"u"&&new OffscreenCanvas(1,1).getContext("2d")!==null}catch{}function M(x,g){return v?new OffscreenCanvas(x,g):Lo("canvas")}function m(x,g,O){let $=1,J=gt(x);if((J.width>O||J.height>O)&&($=O/Math.max(J.width,J.height)),$<1)if(typeof HTMLImageElement<"u"&&x instanceof HTMLImageElement||typeof HTMLCanvasElement<"u"&&x instanceof HTMLCanvasElement||typeof ImageBitmap<"u"&&x instanceof ImageBitmap||typeof VideoFrame<"u"&&x instanceof VideoFrame){let ee=Math.floor($*J.width),oe=Math.floor($*J.height);d===void 0&&(d=M(ee,oe));let G=g?M(ee,oe):d;return G.width=ee,G.height=oe,G.getContext("2d").drawImage(x,0,0,ee,oe),be("WebGLRenderer: Texture has been resized from ("+J.width+"x"+J.height+") to ("+ee+"x"+oe+")."),G}else return"data"in x&&be("WebGLRenderer: Image in DataTexture is too big ("+J.width+"x"+J.height+")."),x;return x}function p(x){return x.generateMipmaps}function S(x){n.generateMipmap(x)}function T(x){return x.isWebGLCubeRenderTarget?n.TEXTURE_CUBE_MAP:x.isWebGL3DRenderTarget?n.TEXTURE_3D:x.isWebGLArrayRenderTarget||x.isCompressedArrayTexture?n.TEXTURE_2D_ARRAY:n.TEXTURE_2D}function E(x,g,O,$,J,ee=!1){if(x!==null){if(n[x]!==void 0)return n[x];be("WebGLRenderer: Attempt to use non-existing WebGL internal format '"+x+"'")}let oe;$&&(oe=e.get("EXT_texture_norm16"),oe||be("WebGLRenderer: Unable to use normalized textures without EXT_texture_norm16 extension"));let G=g;if(g===n.RED&&(O===n.FLOAT&&(G=n.R32F),O===n.HALF_FLOAT&&(G=n.R16F),O===n.UNSIGNED_BYTE&&(G=n.R8),O===n.UNSIGNED_SHORT&&oe&&(G=oe.R16_EXT),O===n.SHORT&&oe&&(G=oe.R16_SNORM_EXT)),g===n.RED_INTEGER&&(O===n.UNSIGNED_BYTE&&(G=n.R8UI),O===n.UNSIGNED_SHORT&&(G=n.R16UI),O===n.UNSIGNED_INT&&(G=n.R32UI),O===n.BYTE&&(G=n.R8I),O===n.SHORT&&(G=n.R16I),O===n.INT&&(G=n.R32I)),g===n.RG&&(O===n.FLOAT&&(G=n.RG32F),O===n.HALF_FLOAT&&(G=n.RG16F),O===n.UNSIGNED_BYTE&&(G=n.RG8),O===n.UNSIGNED_SHORT&&oe&&(G=oe.RG16_EXT),O===n.SHORT&&oe&&(G=oe.RG16_SNORM_EXT)),g===n.RG_INTEGER&&(O===n.UNSIGNED_BYTE&&(G=n.RG8UI),O===n.UNSIGNED_SHORT&&(G=n.RG16UI),O===n.UNSIGNED_INT&&(G=n.RG32UI),O===n.BYTE&&(G=n.RG8I),O===n.SHORT&&(G=n.RG16I),O===n.INT&&(G=n.RG32I)),g===n.RGB_INTEGER&&(O===n.UNSIGNED_BYTE&&(G=n.RGB8UI),O===n.UNSIGNED_SHORT&&(G=n.RGB16UI),O===n.UNSIGNED_INT&&(G=n.RGB32UI),O===n.BYTE&&(G=n.RGB8I),O===n.SHORT&&(G=n.RGB16I),O===n.INT&&(G=n.RGB32I)),g===n.RGBA_INTEGER&&(O===n.UNSIGNED_BYTE&&(G=n.RGBA8UI),O===n.UNSIGNED_SHORT&&(G=n.RGBA16UI),O===n.UNSIGNED_INT&&(G=n.RGBA32UI),O===n.BYTE&&(G=n.RGBA8I),O===n.SHORT&&(G=n.RGBA16I),O===n.INT&&(G=n.RGBA32I)),g===n.RGB&&(O===n.UNSIGNED_SHORT&&oe&&(G=oe.RGB16_EXT),O===n.SHORT&&oe&&(G=oe.RGB16_SNORM_EXT),O===n.UNSIGNED_INT_5_9_9_9_REV&&(G=n.RGB9_E5),O===n.UNSIGNED_INT_10F_11F_11F_REV&&(G=n.R11F_G11F_B10F)),g===n.RGBA){let q=ee?No:ze.getTransfer(J);O===n.FLOAT&&(G=n.RGBA32F),O===n.HALF_FLOAT&&(G=n.RGBA16F),O===n.UNSIGNED_BYTE&&(G=q===Qe?n.SRGB8_ALPHA8:n.RGBA8),O===n.UNSIGNED_SHORT&&oe&&(G=oe.RGBA16_EXT),O===n.SHORT&&oe&&(G=oe.RGBA16_SNORM_EXT),O===n.UNSIGNED_SHORT_4_4_4_4&&(G=n.RGBA4),O===n.UNSIGNED_SHORT_5_5_5_1&&(G=n.RGB5_A1)}return(G===n.R16F||G===n.R32F||G===n.RG16F||G===n.RG32F||G===n.RGBA16F||G===n.RGBA32F)&&e.get("EXT_color_buffer_float"),G}function D(x,g){let O;return x?g===null||g===kn||g===Os?O=n.DEPTH24_STENCIL8:g===Un?O=n.DEPTH32F_STENCIL8:g===Ls&&(O=n.DEPTH24_STENCIL8,be("DepthTexture: 16 bit depth attachment is not supported with stencil. Using 24-bit attachment.")):g===null||g===kn||g===Os?O=n.DEPTH_COMPONENT24:g===Un?O=n.DEPTH_COMPONENT32F:g===Ls&&(O=n.DEPTH_COMPONENT16),O}function b(x,g){return p(x)===!0||x.isFramebufferTexture&&x.minFilter!==Pt&&x.minFilter!==Ot?Math.log2(Math.max(g.width,g.height))+1:x.mipmaps!==void 0&&x.mipmaps.length>0?x.mipmaps.length:x.isCompressedTexture&&Array.isArray(x.image)?g.mipmaps.length:1}function A(x){let g=x.target;g.removeEventListener("dispose",A),w(g),g.isVideoTexture&&u.delete(g),g.isHTMLTexture&&f.delete(g)}function y(x){let g=x.target;g.removeEventListener("dispose",y),I(g)}function w(x){let g=i.get(x);if(g.__webglInit===void 0)return;let O=x.source,$=h.get(O);if($){let J=$[g.__cacheKey];J.usedTimes--,J.usedTimes===0&&L(x),Object.keys($).length===0&&h.delete(O)}i.remove(x)}function L(x){let g=i.get(x);n.deleteTexture(g.__webglTexture);let O=x.source,$=h.get(O);delete $[g.__cacheKey],o.memory.textures--}function I(x){let g=i.get(x);if(x.depthTexture&&(x.depthTexture.dispose(),i.remove(x.depthTexture)),x.isWebGLCubeRenderTarget)for(let $=0;$<6;$++){if(Array.isArray(g.__webglFramebuffer[$]))for(let J=0;J<g.__webglFramebuffer[$].length;J++)n.deleteFramebuffer(g.__webglFramebuffer[$][J]);else n.deleteFramebuffer(g.__webglFramebuffer[$]);g.__webglDepthbuffer&&n.deleteRenderbuffer(g.__webglDepthbuffer[$])}else{if(Array.isArray(g.__webglFramebuffer))for(let $=0;$<g.__webglFramebuffer.length;$++)n.deleteFramebuffer(g.__webglFramebuffer[$]);else n.deleteFramebuffer(g.__webglFramebuffer);if(g.__webglDepthbuffer&&n.deleteRenderbuffer(g.__webglDepthbuffer),g.__webglMultisampledFramebuffer&&n.deleteFramebuffer(g.__webglMultisampledFramebuffer),g.__webglColorRenderbuffer)for(let $=0;$<g.__webglColorRenderbuffer.length;$++)g.__webglColorRenderbuffer[$]&&n.deleteRenderbuffer(g.__webglColorRenderbuffer[$]);g.__webglDepthRenderbuffer&&n.deleteRenderbuffer(g.__webglDepthRenderbuffer)}let O=x.textures;for(let $=0,J=O.length;$<J;$++){let ee=i.get(O[$]);ee.__webglTexture&&(n.deleteTexture(ee.__webglTexture),o.memory.textures--),i.remove(O[$])}i.remove(x)}let k=0;function W(){k=0}function X(){return k}function P(x){k=x}function z(){let x=k;return x>=r.maxTextures&&be("WebGLTextures: Trying to use "+x+" texture units while this GPU supports only "+r.maxTextures),k+=1,x}function F(x){let g=[];return g.push(x.wrapS),g.push(x.wrapT),g.push(x.wrapR||0),g.push(x.magFilter),g.push(x.minFilter),g.push(x.anisotropy),g.push(x.internalFormat),g.push(x.format),g.push(x.type),g.push(x.generateMipmaps),g.push(x.premultiplyAlpha),g.push(x.flipY),g.push(x.unpackAlignment),g.push(x.colorSpace),g.join()}function K(x,g){let O=i.get(x);if(x.isVideoTexture&&ut(x),x.isRenderTargetTexture===!1&&x.isExternalTexture!==!0&&x.version>0&&O.__version!==x.version){let $=x.image;if($===null)be("WebGLRenderer: Texture marked for update but no image data found.");else if($.complete===!1)be("WebGLRenderer: Texture marked for update but image is incomplete");else{Te(O,x,g);return}}else x.isExternalTexture&&(O.__webglTexture=x.sourceTexture?x.sourceTexture:null);t.bindTexture(n.TEXTURE_2D,O.__webglTexture,n.TEXTURE0+g)}function Q(x,g){let O=i.get(x);if(x.isRenderTargetTexture===!1&&x.version>0&&O.__version!==x.version){Te(O,x,g);return}else x.isExternalTexture&&(O.__webglTexture=x.sourceTexture?x.sourceTexture:null);t.bindTexture(n.TEXTURE_2D_ARRAY,O.__webglTexture,n.TEXTURE0+g)}function le(x,g){let O=i.get(x);if(x.isRenderTargetTexture===!1&&x.version>0&&O.__version!==x.version){Te(O,x,g);return}t.bindTexture(n.TEXTURE_3D,O.__webglTexture,n.TEXTURE0+g)}function ye(x,g){let O=i.get(x);if(x.isCubeDepthTexture!==!0&&x.version>0&&O.__version!==x.version){De(O,x,g);return}t.bindTexture(n.TEXTURE_CUBE_MAP,O.__webglTexture,n.TEXTURE0+g)}let Se={[tl]:n.REPEAT,[Yn]:n.CLAMP_TO_EDGE,[nl]:n.MIRRORED_REPEAT},qe={[Pt]:n.NEAREST,[e_]:n.NEAREST_MIPMAP_NEAREST,[Zo]:n.NEAREST_MIPMAP_LINEAR,[Ot]:n.LINEAR,[wl]:n.LINEAR_MIPMAP_NEAREST,[$i]:n.LINEAR_MIPMAP_LINEAR},nt={[i_]:n.NEVER,[c_]:n.ALWAYS,[r_]:n.LESS,[lu]:n.LEQUAL,[s_]:n.EQUAL,[uu]:n.GEQUAL,[o_]:n.GREATER,[a_]:n.NOTEQUAL};function Oe(x,g){if(g.type===Un&&e.has("OES_texture_float_linear")===!1&&(g.magFilter===Ot||g.magFilter===wl||g.magFilter===Zo||g.magFilter===$i||g.minFilter===Ot||g.minFilter===wl||g.minFilter===Zo||g.minFilter===$i)&&be("WebGLRenderer: Unable to use linear filtering with floating point textures. OES_texture_float_linear not supported on this device."),n.texParameteri(x,n.TEXTURE_WRAP_S,Se[g.wrapS]),n.texParameteri(x,n.TEXTURE_WRAP_T,Se[g.wrapT]),(x===n.TEXTURE_3D||x===n.TEXTURE_2D_ARRAY)&&n.texParameteri(x,n.TEXTURE_WRAP_R,Se[g.wrapR]),n.texParameteri(x,n.TEXTURE_MAG_FILTER,qe[g.magFilter]),n.texParameteri(x,n.TEXTURE_MIN_FILTER,qe[g.minFilter]),g.compareFunction&&(n.texParameteri(x,n.TEXTURE_COMPARE_MODE,n.COMPARE_REF_TO_TEXTURE),n.texParameteri(x,n.TEXTURE_COMPARE_FUNC,nt[g.compareFunction])),e.has("EXT_texture_filter_anisotropic")===!0){if(g.magFilter===Pt||g.minFilter!==Zo&&g.minFilter!==$i||g.type===Un&&e.has("OES_texture_float_linear")===!1)return;if(g.anisotropy>1||i.get(g).__currentAnisotropy){let O=e.get("EXT_texture_filter_anisotropic");n.texParameterf(x,O.TEXTURE_MAX_ANISOTROPY_EXT,Math.min(g.anisotropy,r.getMaxAnisotropy())),i.get(g).__currentAnisotropy=g.anisotropy}}}function Y(x,g){let O=!1;x.__webglInit===void 0&&(x.__webglInit=!0,g.addEventListener("dispose",A));let $=g.source,J=h.get($);J===void 0&&(J={},h.set($,J));let ee=F(g);if(ee!==x.__cacheKey){J[ee]===void 0&&(J[ee]={texture:n.createTexture(),usedTimes:0},o.memory.textures++,O=!0),J[ee].usedTimes++;let oe=J[x.__cacheKey];oe!==void 0&&(J[x.__cacheKey].usedTimes--,oe.usedTimes===0&&L(g)),x.__cacheKey=ee,x.__webglTexture=J[ee].texture}return O}function fe(x,g,O){return Math.floor(Math.floor(x/O)/g)}function ie(x,g,O,$){let ee=x.updateRanges;if(ee.length===0)t.texSubImage2D(n.TEXTURE_2D,0,0,0,g.width,g.height,O,$,g.data);else{ee.sort((ge,re)=>ge.start-re.start);let oe=0;for(let ge=1;ge<ee.length;ge++){let re=ee[oe],te=ee[ge],Ce=re.start+re.count,Fe=fe(te.start,g.width,4),Je=fe(re.start,g.width,4);te.start<=Ce+1&&Fe===Je&&fe(te.start+te.count-1,g.width,4)===Fe?re.count=Math.max(re.count,te.start+te.count-re.start):(++oe,ee[oe]=te)}ee.length=oe+1;let G=t.getParameter(n.UNPACK_ROW_LENGTH),q=t.getParameter(n.UNPACK_SKIP_PIXELS),he=t.getParameter(n.UNPACK_SKIP_ROWS);t.pixelStorei(n.UNPACK_ROW_LENGTH,g.width);for(let ge=0,re=ee.length;ge<re;ge++){let te=ee[ge],Ce=Math.floor(te.start/4),Fe=Math.ceil(te.count/4),Je=Ce%g.width,C=Math.floor(Ce/g.width),ne=Fe,j=1;t.pixelStorei(n.UNPACK_SKIP_PIXELS,Je),t.pixelStorei(n.UNPACK_SKIP_ROWS,C),t.texSubImage2D(n.TEXTURE_2D,0,Je,C,ne,j,O,$,g.data)}x.clearUpdateRanges(),t.pixelStorei(n.UNPACK_ROW_LENGTH,G),t.pixelStorei(n.UNPACK_SKIP_PIXELS,q),t.pixelStorei(n.UNPACK_SKIP_ROWS,he)}}function Te(x,g,O){let $=n.TEXTURE_2D;(g.isDataArrayTexture||g.isCompressedArrayTexture)&&($=n.TEXTURE_2D_ARRAY),g.isData3DTexture&&($=n.TEXTURE_3D);let J=Y(x,g),ee=g.source;t.bindTexture($,x.__webglTexture,n.TEXTURE0+O);let oe=i.get(ee);if(ee.version!==oe.__version||J===!0){if(t.activeTexture(n.TEXTURE0+O),(typeof ImageBitmap<"u"&&g.image instanceof ImageBitmap)===!1){let j=ze.getPrimaries(ze.workingColorSpace),pe=g.colorSpace===_i?null:ze.getPrimaries(g.colorSpace),se=g.colorSpace===_i||j===pe?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,g.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,g.premultiplyAlpha),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,se)}t.pixelStorei(n.UNPACK_ALIGNMENT,g.unpackAlignment);let q=m(g.image,!1,r.maxTextureSize);q=ae(g,q);let he=s.convert(g.format,g.colorSpace),ge=s.convert(g.type),re=E(g.internalFormat,he,ge,g.normalized,g.colorSpace,g.isVideoTexture);Oe($,g);let te,Ce=g.mipmaps,Fe=g.isVideoTexture!==!0,Je=oe.__version===void 0||J===!0,C=ee.dataReady,ne=b(g,q);if(g.isDepthTexture)re=D(g.format===qi,g.type),Je&&(Fe?t.texStorage2D(n.TEXTURE_2D,1,re,q.width,q.height):t.texImage2D(n.TEXTURE_2D,0,re,q.width,q.height,0,he,ge,null));else if(g.isDataTexture)if(Ce.length>0){Fe&&Je&&t.texStorage2D(n.TEXTURE_2D,ne,re,Ce[0].width,Ce[0].height);for(let j=0,pe=Ce.length;j<pe;j++)te=Ce[j],Fe?C&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,te.width,te.height,he,ge,te.data):t.texImage2D(n.TEXTURE_2D,j,re,te.width,te.height,0,he,ge,te.data);g.generateMipmaps=!1}else Fe?(Je&&t.texStorage2D(n.TEXTURE_2D,ne,re,q.width,q.height),C&&ie(g,q,he,ge)):t.texImage2D(n.TEXTURE_2D,0,re,q.width,q.height,0,he,ge,q.data);else if(g.isCompressedTexture)if(g.isCompressedArrayTexture){Fe&&Je&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ne,re,Ce[0].width,Ce[0].height,q.depth);for(let j=0,pe=Ce.length;j<pe;j++)if(te=Ce[j],g.format!==bn)if(he!==null)if(Fe){if(C)if(g.layerUpdates.size>0){let se=dp(te.width,te.height,g.format,g.type);for(let Z of g.layerUpdates){let xe=te.data.subarray(Z*se/te.data.BYTES_PER_ELEMENT,(Z+1)*se/te.data.BYTES_PER_ELEMENT);t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,Z,te.width,te.height,1,he,xe)}g.clearLayerUpdates()}else t.compressedTexSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,0,te.width,te.height,q.depth,he,te.data)}else t.compressedTexImage3D(n.TEXTURE_2D_ARRAY,j,re,te.width,te.height,q.depth,0,te.data,0,0);else be("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()");else Fe?C&&t.texSubImage3D(n.TEXTURE_2D_ARRAY,j,0,0,0,te.width,te.height,q.depth,he,ge,te.data):t.texImage3D(n.TEXTURE_2D_ARRAY,j,re,te.width,te.height,q.depth,0,he,ge,te.data)}else{Fe&&Je&&t.texStorage2D(n.TEXTURE_2D,ne,re,Ce[0].width,Ce[0].height);for(let j=0,pe=Ce.length;j<pe;j++)te=Ce[j],g.format!==bn?he!==null?Fe?C&&t.compressedTexSubImage2D(n.TEXTURE_2D,j,0,0,te.width,te.height,he,te.data):t.compressedTexImage2D(n.TEXTURE_2D,j,re,te.width,te.height,0,te.data):be("WebGLRenderer: Attempt to load unsupported compressed texture format in .uploadTexture()"):Fe?C&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,te.width,te.height,he,ge,te.data):t.texImage2D(n.TEXTURE_2D,j,re,te.width,te.height,0,he,ge,te.data)}else if(g.isDataArrayTexture)if(Fe){if(Je&&t.texStorage3D(n.TEXTURE_2D_ARRAY,ne,re,q.width,q.height,q.depth),C)if(g.layerUpdates.size>0){let j=dp(q.width,q.height,g.format,g.type);for(let pe of g.layerUpdates){let se=q.data.subarray(pe*j/q.data.BYTES_PER_ELEMENT,(pe+1)*j/q.data.BYTES_PER_ELEMENT);t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,pe,q.width,q.height,1,he,ge,se)}g.clearLayerUpdates()}else t.texSubImage3D(n.TEXTURE_2D_ARRAY,0,0,0,0,q.width,q.height,q.depth,he,ge,q.data)}else t.texImage3D(n.TEXTURE_2D_ARRAY,0,re,q.width,q.height,q.depth,0,he,ge,q.data);else if(g.isData3DTexture)Fe?(Je&&t.texStorage3D(n.TEXTURE_3D,ne,re,q.width,q.height,q.depth),C&&t.texSubImage3D(n.TEXTURE_3D,0,0,0,0,q.width,q.height,q.depth,he,ge,q.data)):t.texImage3D(n.TEXTURE_3D,0,re,q.width,q.height,q.depth,0,he,ge,q.data);else if(g.isFramebufferTexture){if(Je)if(Fe)t.texStorage2D(n.TEXTURE_2D,ne,re,q.width,q.height);else{let j=q.width,pe=q.height;for(let se=0;se<ne;se++)t.texImage2D(n.TEXTURE_2D,se,re,j,pe,0,he,ge,null),j>>=1,pe>>=1}}else if(g.isHTMLTexture){if("texElementImage2D"in n){let j=n.canvas;if(j.hasAttribute("layoutsubtree")||j.setAttribute("layoutsubtree","true"),q.parentNode!==j){j.appendChild(q),f.add(g),j.onpaint=Ne=>{let xt=Ne.changedElements;for(let rt of f)xt.includes(rt.image)&&(rt.needsUpdate=!0)},j.requestPaint();return}let pe=0,se=n.RGBA,Z=n.RGBA,xe=n.UNSIGNED_BYTE;n.texElementImage2D(n.TEXTURE_2D,pe,se,Z,xe,q),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_MIN_FILTER,n.LINEAR),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_S,n.CLAMP_TO_EDGE),n.texParameteri(n.TEXTURE_2D,n.TEXTURE_WRAP_T,n.CLAMP_TO_EDGE)}}else if(Ce.length>0){if(Fe&&Je){let j=gt(Ce[0]);t.texStorage2D(n.TEXTURE_2D,ne,re,j.width,j.height)}for(let j=0,pe=Ce.length;j<pe;j++)te=Ce[j],Fe?C&&t.texSubImage2D(n.TEXTURE_2D,j,0,0,he,ge,te):t.texImage2D(n.TEXTURE_2D,j,re,he,ge,te);g.generateMipmaps=!1}else if(Fe){if(Je){let j=gt(q);t.texStorage2D(n.TEXTURE_2D,ne,re,j.width,j.height)}C&&t.texSubImage2D(n.TEXTURE_2D,0,0,0,he,ge,q)}else t.texImage2D(n.TEXTURE_2D,0,re,he,ge,q);p(g)&&S($),oe.__version=ee.version,g.onUpdate&&g.onUpdate(g)}x.__version=g.version}function De(x,g,O){if(g.image.length!==6)return;let $=Y(x,g),J=g.source;t.bindTexture(n.TEXTURE_CUBE_MAP,x.__webglTexture,n.TEXTURE0+O);let ee=i.get(J);if(J.version!==ee.__version||$===!0){t.activeTexture(n.TEXTURE0+O);let oe=ze.getPrimaries(ze.workingColorSpace),G=g.colorSpace===_i?null:ze.getPrimaries(g.colorSpace),q=g.colorSpace===_i||oe===G?n.NONE:n.BROWSER_DEFAULT_WEBGL;t.pixelStorei(n.UNPACK_FLIP_Y_WEBGL,g.flipY),t.pixelStorei(n.UNPACK_PREMULTIPLY_ALPHA_WEBGL,g.premultiplyAlpha),t.pixelStorei(n.UNPACK_ALIGNMENT,g.unpackAlignment),t.pixelStorei(n.UNPACK_COLORSPACE_CONVERSION_WEBGL,q);let he=g.isCompressedTexture||g.image[0].isCompressedTexture,ge=g.image[0]&&g.image[0].isDataTexture,re=[];for(let Z=0;Z<6;Z++)!he&&!ge?re[Z]=m(g.image[Z],!0,r.maxCubemapSize):re[Z]=ge?g.image[Z].image:g.image[Z],re[Z]=ae(g,re[Z]);let te=re[0],Ce=s.convert(g.format,g.colorSpace),Fe=s.convert(g.type),Je=E(g.internalFormat,Ce,Fe,g.normalized,g.colorSpace),C=g.isVideoTexture!==!0,ne=ee.__version===void 0||$===!0,j=J.dataReady,pe=b(g,te);Oe(n.TEXTURE_CUBE_MAP,g);let se;if(he){C&&ne&&t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,Je,te.width,te.height);for(let Z=0;Z<6;Z++){se=re[Z].mipmaps;for(let xe=0;xe<se.length;xe++){let Ne=se[xe];g.format!==bn?Ce!==null?C?j&&t.compressedTexSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,0,0,Ne.width,Ne.height,Ce,Ne.data):t.compressedTexImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,Je,Ne.width,Ne.height,0,Ne.data):be("WebGLRenderer: Attempt to load unsupported compressed texture format in .setTextureCube()"):C?j&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,0,0,Ne.width,Ne.height,Ce,Fe,Ne.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe,Je,Ne.width,Ne.height,0,Ce,Fe,Ne.data)}}}else{if(se=g.mipmaps,C&&ne){se.length>0&&pe++;let Z=gt(re[0]);t.texStorage2D(n.TEXTURE_CUBE_MAP,pe,Je,Z.width,Z.height)}for(let Z=0;Z<6;Z++)if(ge){C?j&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,re[Z].width,re[Z].height,Ce,Fe,re[Z].data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Je,re[Z].width,re[Z].height,0,Ce,Fe,re[Z].data);for(let xe=0;xe<se.length;xe++){let xt=se[xe].image[Z].image;C?j&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,0,0,xt.width,xt.height,Ce,Fe,xt.data):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,Je,xt.width,xt.height,0,Ce,Fe,xt.data)}}else{C?j&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,0,0,Ce,Fe,re[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,0,Je,Ce,Fe,re[Z]);for(let xe=0;xe<se.length;xe++){let Ne=se[xe];C?j&&t.texSubImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,0,0,Ce,Fe,Ne.image[Z]):t.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+Z,xe+1,Je,Ce,Fe,Ne.image[Z])}}}p(g)&&S(n.TEXTURE_CUBE_MAP),ee.__version=J.version,g.onUpdate&&g.onUpdate(g)}x.__version=g.version}function Ie(x,g,O,$,J,ee){let oe=s.convert(O.format,O.colorSpace),G=s.convert(O.type),q=E(O.internalFormat,oe,G,O.normalized,O.colorSpace),he=i.get(g),ge=i.get(O);if(ge.__renderTarget=g,!he.__hasExternalTextures){let re=Math.max(1,g.width>>ee),te=Math.max(1,g.height>>ee);J===n.TEXTURE_3D||J===n.TEXTURE_2D_ARRAY?t.texImage3D(J,ee,q,re,te,g.depth,0,oe,G,null):t.texImage2D(J,ee,q,re,te,0,oe,G,null)}t.bindFramebuffer(n.FRAMEBUFFER,x),He(g)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,$,J,ge.__webglTexture,0,It(g)):(J===n.TEXTURE_2D||J>=n.TEXTURE_CUBE_MAP_POSITIVE_X&&J<=n.TEXTURE_CUBE_MAP_NEGATIVE_Z)&&n.framebufferTexture2D(n.FRAMEBUFFER,$,J,ge.__webglTexture,ee),t.bindFramebuffer(n.FRAMEBUFFER,null)}function pt(x,g,O){if(n.bindRenderbuffer(n.RENDERBUFFER,x),g.depthBuffer){let $=g.depthTexture,J=$&&$.isDepthTexture?$.type:null,ee=D(g.stencilBuffer,J),oe=g.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;He(g)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,It(g),ee,g.width,g.height):O?n.renderbufferStorageMultisample(n.RENDERBUFFER,It(g),ee,g.width,g.height):n.renderbufferStorage(n.RENDERBUFFER,ee,g.width,g.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,oe,n.RENDERBUFFER,x)}else{let $=g.textures;for(let J=0;J<$.length;J++){let ee=$[J],oe=s.convert(ee.format,ee.colorSpace),G=s.convert(ee.type),q=E(ee.internalFormat,oe,G,ee.normalized,ee.colorSpace);He(g)?a.renderbufferStorageMultisampleEXT(n.RENDERBUFFER,It(g),q,g.width,g.height):O?n.renderbufferStorageMultisample(n.RENDERBUFFER,It(g),q,g.width,g.height):n.renderbufferStorage(n.RENDERBUFFER,q,g.width,g.height)}}n.bindRenderbuffer(n.RENDERBUFFER,null)}function Ve(x,g,O){let $=g.isWebGLCubeRenderTarget===!0;if(t.bindFramebuffer(n.FRAMEBUFFER,x),!(g.depthTexture&&g.depthTexture.isDepthTexture))throw new Error("renderTarget.depthTexture must be an instance of THREE.DepthTexture");let J=i.get(g.depthTexture);if(J.__renderTarget=g,(!J.__webglTexture||g.depthTexture.image.width!==g.width||g.depthTexture.image.height!==g.height)&&(g.depthTexture.image.width=g.width,g.depthTexture.image.height=g.height,g.depthTexture.needsUpdate=!0),$){if(J.__webglInit===void 0&&(J.__webglInit=!0,g.depthTexture.addEventListener("dispose",A)),J.__webglTexture===void 0){J.__webglTexture=n.createTexture(),t.bindTexture(n.TEXTURE_CUBE_MAP,J.__webglTexture),Oe(n.TEXTURE_CUBE_MAP,g.depthTexture);let he=s.convert(g.depthTexture.format),ge=s.convert(g.depthTexture.type),re;g.depthTexture.format===Zn?re=n.DEPTH_COMPONENT24:g.depthTexture.format===qi&&(re=n.DEPTH24_STENCIL8);for(let te=0;te<6;te++)n.texImage2D(n.TEXTURE_CUBE_MAP_POSITIVE_X+te,0,re,g.width,g.height,0,he,ge,null)}}else K(g.depthTexture,0);let ee=J.__webglTexture,oe=It(g),G=$?n.TEXTURE_CUBE_MAP_POSITIVE_X+O:n.TEXTURE_2D,q=g.depthTexture.format===qi?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;if(g.depthTexture.format===Zn)He(g)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,G,ee,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,q,G,ee,0);else if(g.depthTexture.format===qi)He(g)?a.framebufferTexture2DMultisampleEXT(n.FRAMEBUFFER,q,G,ee,0,oe):n.framebufferTexture2D(n.FRAMEBUFFER,q,G,ee,0);else throw new Error("Unknown depthTexture format")}function it(x){let g=i.get(x),O=x.isWebGLCubeRenderTarget===!0;if(g.__boundDepthTexture!==x.depthTexture){let $=x.depthTexture;if(g.__depthDisposeCallback&&g.__depthDisposeCallback(),$){let J=()=>{delete g.__boundDepthTexture,delete g.__depthDisposeCallback,$.removeEventListener("dispose",J)};$.addEventListener("dispose",J),g.__depthDisposeCallback=J}g.__boundDepthTexture=$}if(x.depthTexture&&!g.__autoAllocateDepthBuffer)if(O)for(let $=0;$<6;$++)Ve(g.__webglFramebuffer[$],x,$);else{let $=x.texture.mipmaps;$&&$.length>0?Ve(g.__webglFramebuffer[0],x,0):Ve(g.__webglFramebuffer,x,0)}else if(O){g.__webglDepthbuffer=[];for(let $=0;$<6;$++)if(t.bindFramebuffer(n.FRAMEBUFFER,g.__webglFramebuffer[$]),g.__webglDepthbuffer[$]===void 0)g.__webglDepthbuffer[$]=n.createRenderbuffer(),pt(g.__webglDepthbuffer[$],x,!1);else{let J=x.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ee=g.__webglDepthbuffer[$];n.bindRenderbuffer(n.RENDERBUFFER,ee),n.framebufferRenderbuffer(n.FRAMEBUFFER,J,n.RENDERBUFFER,ee)}}else{let $=x.texture.mipmaps;if($&&$.length>0?t.bindFramebuffer(n.FRAMEBUFFER,g.__webglFramebuffer[0]):t.bindFramebuffer(n.FRAMEBUFFER,g.__webglFramebuffer),g.__webglDepthbuffer===void 0)g.__webglDepthbuffer=n.createRenderbuffer(),pt(g.__webglDepthbuffer,x,!1);else{let J=x.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,ee=g.__webglDepthbuffer;n.bindRenderbuffer(n.RENDERBUFFER,ee),n.framebufferRenderbuffer(n.FRAMEBUFFER,J,n.RENDERBUFFER,ee)}}t.bindFramebuffer(n.FRAMEBUFFER,null)}function ht(x,g,O){let $=i.get(x);g!==void 0&&Ie($.__webglFramebuffer,x,x.texture,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,0),O!==void 0&&it(x)}function Be(x){let g=x.texture,O=i.get(x),$=i.get(g);x.addEventListener("dispose",y);let J=x.textures,ee=x.isWebGLCubeRenderTarget===!0,oe=J.length>1;if(oe||($.__webglTexture===void 0&&($.__webglTexture=n.createTexture()),$.__version=g.version,o.memory.textures++),ee){O.__webglFramebuffer=[];for(let G=0;G<6;G++)if(g.mipmaps&&g.mipmaps.length>0){O.__webglFramebuffer[G]=[];for(let q=0;q<g.mipmaps.length;q++)O.__webglFramebuffer[G][q]=n.createFramebuffer()}else O.__webglFramebuffer[G]=n.createFramebuffer()}else{if(g.mipmaps&&g.mipmaps.length>0){O.__webglFramebuffer=[];for(let G=0;G<g.mipmaps.length;G++)O.__webglFramebuffer[G]=n.createFramebuffer()}else O.__webglFramebuffer=n.createFramebuffer();if(oe)for(let G=0,q=J.length;G<q;G++){let he=i.get(J[G]);he.__webglTexture===void 0&&(he.__webglTexture=n.createTexture(),o.memory.textures++)}if(x.samples>0&&He(x)===!1){O.__webglMultisampledFramebuffer=n.createFramebuffer(),O.__webglColorRenderbuffer=[],t.bindFramebuffer(n.FRAMEBUFFER,O.__webglMultisampledFramebuffer);for(let G=0;G<J.length;G++){let q=J[G];O.__webglColorRenderbuffer[G]=n.createRenderbuffer(),n.bindRenderbuffer(n.RENDERBUFFER,O.__webglColorRenderbuffer[G]);let he=s.convert(q.format,q.colorSpace),ge=s.convert(q.type),re=E(q.internalFormat,he,ge,q.normalized,q.colorSpace,x.isXRRenderTarget===!0),te=It(x);n.renderbufferStorageMultisample(n.RENDERBUFFER,te,re,x.width,x.height),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+G,n.RENDERBUFFER,O.__webglColorRenderbuffer[G])}n.bindRenderbuffer(n.RENDERBUFFER,null),x.depthBuffer&&(O.__webglDepthRenderbuffer=n.createRenderbuffer(),pt(O.__webglDepthRenderbuffer,x,!0)),t.bindFramebuffer(n.FRAMEBUFFER,null)}}if(ee){t.bindTexture(n.TEXTURE_CUBE_MAP,$.__webglTexture),Oe(n.TEXTURE_CUBE_MAP,g);for(let G=0;G<6;G++)if(g.mipmaps&&g.mipmaps.length>0)for(let q=0;q<g.mipmaps.length;q++)Ie(O.__webglFramebuffer[G][q],x,g,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,q);else Ie(O.__webglFramebuffer[G],x,g,n.COLOR_ATTACHMENT0,n.TEXTURE_CUBE_MAP_POSITIVE_X+G,0);p(g)&&S(n.TEXTURE_CUBE_MAP),t.unbindTexture()}else if(oe){for(let G=0,q=J.length;G<q;G++){let he=J[G],ge=i.get(he),re=n.TEXTURE_2D;(x.isWebGL3DRenderTarget||x.isWebGLArrayRenderTarget)&&(re=x.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(re,ge.__webglTexture),Oe(re,he),Ie(O.__webglFramebuffer,x,he,n.COLOR_ATTACHMENT0+G,re,0),p(he)&&S(re)}t.unbindTexture()}else{let G=n.TEXTURE_2D;if((x.isWebGL3DRenderTarget||x.isWebGLArrayRenderTarget)&&(G=x.isWebGL3DRenderTarget?n.TEXTURE_3D:n.TEXTURE_2D_ARRAY),t.bindTexture(G,$.__webglTexture),Oe(G,g),g.mipmaps&&g.mipmaps.length>0)for(let q=0;q<g.mipmaps.length;q++)Ie(O.__webglFramebuffer[q],x,g,n.COLOR_ATTACHMENT0,G,q);else Ie(O.__webglFramebuffer,x,g,n.COLOR_ATTACHMENT0,G,0);p(g)&&S(G),t.unbindTexture()}x.depthBuffer&&it(x)}function wt(x){let g=x.textures;for(let O=0,$=g.length;O<$;O++){let J=g[O];if(p(J)){let ee=T(x),oe=i.get(J).__webglTexture;t.bindTexture(ee,oe),S(ee),t.unbindTexture()}}}let mt=[],en=[];function R(x){if(x.samples>0){if(He(x)===!1){let g=x.textures,O=x.width,$=x.height,J=n.COLOR_BUFFER_BIT,ee=x.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT,oe=i.get(x),G=g.length>1;if(G)for(let he=0;he<g.length;he++)t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,null),t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,null,0);t.bindFramebuffer(n.READ_FRAMEBUFFER,oe.__webglMultisampledFramebuffer);let q=x.texture.mipmaps;q&&q.length>0?t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer[0]):t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglFramebuffer);for(let he=0;he<g.length;he++){if(x.resolveDepthBuffer&&(x.depthBuffer&&(J|=n.DEPTH_BUFFER_BIT),x.stencilBuffer&&x.resolveStencilBuffer&&(J|=n.STENCIL_BUFFER_BIT)),G){n.framebufferRenderbuffer(n.READ_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ge=i.get(g[he]).__webglTexture;n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0,n.TEXTURE_2D,ge,0)}n.blitFramebuffer(0,0,O,$,0,0,O,$,J,n.NEAREST),c===!0&&(mt.length=0,en.length=0,mt.push(n.COLOR_ATTACHMENT0+he),x.depthBuffer&&x.resolveDepthBuffer===!1&&(mt.push(ee),en.push(ee),n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,en)),n.invalidateFramebuffer(n.READ_FRAMEBUFFER,mt))}if(t.bindFramebuffer(n.READ_FRAMEBUFFER,null),t.bindFramebuffer(n.DRAW_FRAMEBUFFER,null),G)for(let he=0;he<g.length;he++){t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglMultisampledFramebuffer),n.framebufferRenderbuffer(n.FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.RENDERBUFFER,oe.__webglColorRenderbuffer[he]);let ge=i.get(g[he]).__webglTexture;t.bindFramebuffer(n.FRAMEBUFFER,oe.__webglFramebuffer),n.framebufferTexture2D(n.DRAW_FRAMEBUFFER,n.COLOR_ATTACHMENT0+he,n.TEXTURE_2D,ge,0)}t.bindFramebuffer(n.DRAW_FRAMEBUFFER,oe.__webglMultisampledFramebuffer)}else if(x.depthBuffer&&x.resolveDepthBuffer===!1&&c){let g=x.stencilBuffer?n.DEPTH_STENCIL_ATTACHMENT:n.DEPTH_ATTACHMENT;n.invalidateFramebuffer(n.DRAW_FRAMEBUFFER,[g])}}}function It(x){return Math.min(r.maxSamples,x.samples)}function He(x){let g=i.get(x);return x.samples>0&&e.has("WEBGL_multisampled_render_to_texture")===!0&&g.__useRenderToTexture!==!1}function ut(x){let g=o.render.frame;u.get(x)!==g&&(u.set(x,g),x.update())}function ae(x,g){let O=x.colorSpace,$=x.format,J=x.type;return x.isCompressedTexture===!0||x.isVideoTexture===!0||O!==Ro&&O!==_i&&(ze.getTransfer(O)===Qe?($!==bn||J!==pn)&&be("WebGLTextures: sRGB encoded textures have to use RGBAFormat and UnsignedByteType."):we("WebGLTextures: Unsupported texture color space:",O)),g}function gt(x){return typeof HTMLImageElement<"u"&&x instanceof HTMLImageElement?(l.width=x.naturalWidth||x.width,l.height=x.naturalHeight||x.height):typeof VideoFrame<"u"&&x instanceof VideoFrame?(l.width=x.displayWidth,l.height=x.displayHeight):(l.width=x.width,l.height=x.height),l}this.allocateTextureUnit=z,this.resetTextureUnits=W,this.getTextureUnits=X,this.setTextureUnits=P,this.setTexture2D=K,this.setTexture2DArray=Q,this.setTexture3D=le,this.setTextureCube=ye,this.rebindTextures=ht,this.setupRenderTarget=Be,this.updateRenderTargetMipmap=wt,this.updateMultisampleRenderTarget=R,this.setupDepthRenderbuffer=it,this.setupFrameBufferTexture=Ie,this.useMultisampledRTT=He,this.isReversedDepthBuffer=function(){return t.buffers.depth.getReversed()}}function hA(n,e){function t(i,r=_i){let s,o=ze.getTransfer(r);if(i===pn)return n.UNSIGNED_BYTE;if(i===Cl)return n.UNSIGNED_SHORT_4_4_4_4;if(i===Al)return n.UNSIGNED_SHORT_5_5_5_1;if(i===ep)return n.UNSIGNED_INT_5_9_9_9_REV;if(i===tp)return n.UNSIGNED_INT_10F_11F_11F_REV;if(i===Kh)return n.BYTE;if(i===Qh)return n.SHORT;if(i===Ls)return n.UNSIGNED_SHORT;if(i===Il)return n.INT;if(i===kn)return n.UNSIGNED_INT;if(i===Un)return n.FLOAT;if(i===ei)return n.HALF_FLOAT;if(i===np)return n.ALPHA;if(i===ip)return n.RGB;if(i===bn)return n.RGBA;if(i===Zn)return n.DEPTH_COMPONENT;if(i===qi)return n.DEPTH_STENCIL;if(i===rp)return n.RED;if(i===Dl)return n.RED_INTEGER;if(i===Xi)return n.RG;if(i===Rl)return n.RG_INTEGER;if(i===Nl)return n.RGBA_INTEGER;if(i===Jo||i===Ko||i===Qo||i===ea)if(o===Qe)if(s=e.get("WEBGL_compressed_texture_s3tc_srgb"),s!==null){if(i===Jo)return s.COMPRESSED_SRGB_S3TC_DXT1_EXT;if(i===Ko)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT1_EXT;if(i===Qo)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT3_EXT;if(i===ea)return s.COMPRESSED_SRGB_ALPHA_S3TC_DXT5_EXT}else return null;else if(s=e.get("WEBGL_compressed_texture_s3tc"),s!==null){if(i===Jo)return s.COMPRESSED_RGB_S3TC_DXT1_EXT;if(i===Ko)return s.COMPRESSED_RGBA_S3TC_DXT1_EXT;if(i===Qo)return s.COMPRESSED_RGBA_S3TC_DXT3_EXT;if(i===ea)return s.COMPRESSED_RGBA_S3TC_DXT5_EXT}else return null;if(i===Pl||i===Ll||i===Ol||i===Fl)if(s=e.get("WEBGL_compressed_texture_pvrtc"),s!==null){if(i===Pl)return s.COMPRESSED_RGB_PVRTC_4BPPV1_IMG;if(i===Ll)return s.COMPRESSED_RGB_PVRTC_2BPPV1_IMG;if(i===Ol)return s.COMPRESSED_RGBA_PVRTC_4BPPV1_IMG;if(i===Fl)return s.COMPRESSED_RGBA_PVRTC_2BPPV1_IMG}else return null;if(i===kl||i===Ul||i===Bl||i===Vl||i===Hl||i===ta||i===zl)if(s=e.get("WEBGL_compressed_texture_etc"),s!==null){if(i===kl||i===Ul)return o===Qe?s.COMPRESSED_SRGB8_ETC2:s.COMPRESSED_RGB8_ETC2;if(i===Bl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ETC2_EAC:s.COMPRESSED_RGBA8_ETC2_EAC;if(i===Vl)return s.COMPRESSED_R11_EAC;if(i===Hl)return s.COMPRESSED_SIGNED_R11_EAC;if(i===ta)return s.COMPRESSED_RG11_EAC;if(i===zl)return s.COMPRESSED_SIGNED_RG11_EAC}else return null;if(i===Gl||i===Wl||i===jl||i===$l||i===ql||i===Xl||i===Yl||i===Zl||i===Jl||i===Kl||i===Ql||i===eu||i===tu||i===nu)if(s=e.get("WEBGL_compressed_texture_astc"),s!==null){if(i===Gl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_4x4_KHR:s.COMPRESSED_RGBA_ASTC_4x4_KHR;if(i===Wl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x4_KHR:s.COMPRESSED_RGBA_ASTC_5x4_KHR;if(i===jl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_5x5_KHR:s.COMPRESSED_RGBA_ASTC_5x5_KHR;if(i===$l)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x5_KHR:s.COMPRESSED_RGBA_ASTC_6x5_KHR;if(i===ql)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_6x6_KHR:s.COMPRESSED_RGBA_ASTC_6x6_KHR;if(i===Xl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x5_KHR:s.COMPRESSED_RGBA_ASTC_8x5_KHR;if(i===Yl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x6_KHR:s.COMPRESSED_RGBA_ASTC_8x6_KHR;if(i===Zl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_8x8_KHR:s.COMPRESSED_RGBA_ASTC_8x8_KHR;if(i===Jl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x5_KHR:s.COMPRESSED_RGBA_ASTC_10x5_KHR;if(i===Kl)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x6_KHR:s.COMPRESSED_RGBA_ASTC_10x6_KHR;if(i===Ql)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x8_KHR:s.COMPRESSED_RGBA_ASTC_10x8_KHR;if(i===eu)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_10x10_KHR:s.COMPRESSED_RGBA_ASTC_10x10_KHR;if(i===tu)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x10_KHR:s.COMPRESSED_RGBA_ASTC_12x10_KHR;if(i===nu)return o===Qe?s.COMPRESSED_SRGB8_ALPHA8_ASTC_12x12_KHR:s.COMPRESSED_RGBA_ASTC_12x12_KHR}else return null;if(i===iu||i===ru||i===su)if(s=e.get("EXT_texture_compression_bptc"),s!==null){if(i===iu)return o===Qe?s.COMPRESSED_SRGB_ALPHA_BPTC_UNORM_EXT:s.COMPRESSED_RGBA_BPTC_UNORM_EXT;if(i===ru)return s.COMPRESSED_RGB_BPTC_SIGNED_FLOAT_EXT;if(i===su)return s.COMPRESSED_RGB_BPTC_UNSIGNED_FLOAT_EXT}else return null;if(i===ou||i===au||i===na||i===cu)if(s=e.get("EXT_texture_compression_rgtc"),s!==null){if(i===ou)return s.COMPRESSED_RED_RGTC1_EXT;if(i===au)return s.COMPRESSED_SIGNED_RED_RGTC1_EXT;if(i===na)return s.COMPRESSED_RED_GREEN_RGTC2_EXT;if(i===cu)return s.COMPRESSED_SIGNED_RED_GREEN_RGTC2_EXT}else return null;return i===Os?n.UNSIGNED_INT_24_8:n[i]!==void 0?n[i]:null}return{convert:t}}var pA=`
void main() {

	gl_Position = vec4( position, 1.0 );

}`,mA=`
uniform sampler2DArray depthColor;
uniform float depthWidth;
uniform float depthHeight;

void main() {

	vec2 coord = vec2( gl_FragCoord.x / depthWidth, gl_FragCoord.y / depthHeight );

	if ( coord.x >= 1.0 ) {

		gl_FragDepth = texture( depthColor, vec3( coord.x - 1.0, coord.y, 1 ) ).r;

	} else {

		gl_FragDepth = texture( depthColor, vec3( coord.x, coord.y, 0 ) ).r;

	}

}`,Tp=class{constructor(){this.texture=null,this.mesh=null,this.depthNear=0,this.depthFar=0}init(e,t){if(this.texture===null){let i=new Wo(e.texture);(e.depthNear!==t.depthNear||e.depthFar!==t.depthFar)&&(this.depthNear=e.depthNear,this.depthFar=e.depthFar),this.texture=i}}getMesh(e){if(this.texture!==null&&this.mesh===null){let t=e.cameras[0].viewport,i=new fn({vertexShader:pA,fragmentShader:mA,uniforms:{depthColor:{value:this.texture},depthWidth:{value:t.z},depthHeight:{value:t.w}}});this.mesh=new Qt(new jo(20,20),i)}return this.mesh}reset(){this.texture=null,this.mesh=null}getDepthTexture(){return this.texture}},wp=class extends Jn{constructor(e,t){super();let i=this,r=null,s=1,o=null,a="local-floor",c=1,l=null,u=null,f=null,d=null,h=null,v=null,M=typeof XRWebGLBinding<"u",m=new Tp,p={},S=t.getContextAttributes(),T=null,E=null,D=[],b=[],A=new tt,y=null,w=new Kt;w.viewport=new _t;let L=new Kt;L.viewport=new _t;let I=[w,L],k=new El,W=null,X=null;this.cameraAutoUpdate=!0,this.enabled=!1,this.isPresenting=!1,this.getController=function(Y){let fe=D[Y];return fe===void 0&&(fe=new As,D[Y]=fe),fe.getTargetRaySpace()},this.getControllerGrip=function(Y){let fe=D[Y];return fe===void 0&&(fe=new As,D[Y]=fe),fe.getGripSpace()},this.getHand=function(Y){let fe=D[Y];return fe===void 0&&(fe=new As,D[Y]=fe),fe.getHandSpace()};function P(Y){let fe=b.indexOf(Y.inputSource);if(fe===-1)return;let ie=D[fe];ie!==void 0&&(ie.update(Y.inputSource,Y.frame,l||o),ie.dispatchEvent({type:Y.type,data:Y.inputSource}))}function z(){r.removeEventListener("select",P),r.removeEventListener("selectstart",P),r.removeEventListener("selectend",P),r.removeEventListener("squeeze",P),r.removeEventListener("squeezestart",P),r.removeEventListener("squeezeend",P),r.removeEventListener("end",z),r.removeEventListener("inputsourceschange",F);for(let Y=0;Y<D.length;Y++){let fe=b[Y];fe!==null&&(b[Y]=null,D[Y].disconnect(fe))}W=null,X=null,m.reset();for(let Y in p)delete p[Y];e.setRenderTarget(T),h=null,d=null,f=null,r=null,E=null,Oe.stop(),i.isPresenting=!1,e.setPixelRatio(y),e.setSize(A.width,A.height,!1),i.dispatchEvent({type:"sessionend"})}this.setFramebufferScaleFactor=function(Y){s=Y,i.isPresenting===!0&&be("WebXRManager: Cannot change framebuffer scale while presenting.")},this.setReferenceSpaceType=function(Y){a=Y,i.isPresenting===!0&&be("WebXRManager: Cannot change reference space type while presenting.")},this.getReferenceSpace=function(){return l||o},this.setReferenceSpace=function(Y){l=Y},this.getBaseLayer=function(){return d!==null?d:h},this.getBinding=function(){return f===null&&M&&(f=new XRWebGLBinding(r,t)),f},this.getFrame=function(){return v},this.getSession=function(){return r},this.setSession=async function(Y){if(r=Y,r!==null){if(T=e.getRenderTarget(),r.addEventListener("select",P),r.addEventListener("selectstart",P),r.addEventListener("selectend",P),r.addEventListener("squeeze",P),r.addEventListener("squeezestart",P),r.addEventListener("squeezeend",P),r.addEventListener("end",z),r.addEventListener("inputsourceschange",F),S.xrCompatible!==!0&&await t.makeXRCompatible(),y=e.getPixelRatio(),e.getSize(A),M&&"createProjectionLayer"in XRWebGLBinding.prototype){let ie=null,Te=null,De=null;S.depth&&(De=S.stencil?t.DEPTH24_STENCIL8:t.DEPTH_COMPONENT24,ie=S.stencil?qi:Zn,Te=S.stencil?Os:kn);let Ie={colorFormat:t.RGBA8,depthFormat:De,scaleFactor:s};f=this.getBinding(),d=f.createProjectionLayer(Ie),r.updateRenderState({layers:[d]}),e.setPixelRatio(1),e.setSize(d.textureWidth,d.textureHeight,!1),E=new dn(d.textureWidth,d.textureHeight,{format:bn,type:pn,depthTexture:new yi(d.textureWidth,d.textureHeight,Te,void 0,void 0,void 0,void 0,void 0,void 0,ie),stencilBuffer:S.stencil,colorSpace:e.outputColorSpace,samples:S.antialias?4:0,resolveDepthBuffer:d.ignoreDepthValues===!1,resolveStencilBuffer:d.ignoreDepthValues===!1})}else{let ie={antialias:S.antialias,alpha:!0,depth:S.depth,stencil:S.stencil,framebufferScaleFactor:s};h=new XRWebGLLayer(r,t,ie),r.updateRenderState({baseLayer:h}),e.setPixelRatio(1),e.setSize(h.framebufferWidth,h.framebufferHeight,!1),E=new dn(h.framebufferWidth,h.framebufferHeight,{format:bn,type:pn,colorSpace:e.outputColorSpace,stencilBuffer:S.stencil,resolveDepthBuffer:h.ignoreDepthValues===!1,resolveStencilBuffer:h.ignoreDepthValues===!1})}E.isXRRenderTarget=!0,this.setFoveation(c),l=null,o=await r.requestReferenceSpace(a),Oe.setContext(r),Oe.start(),i.isPresenting=!0,i.dispatchEvent({type:"sessionstart"})}},this.getEnvironmentBlendMode=function(){if(r!==null)return r.environmentBlendMode},this.getDepthTexture=function(){return m.getDepthTexture()};function F(Y){for(let fe=0;fe<Y.removed.length;fe++){let ie=Y.removed[fe],Te=b.indexOf(ie);Te>=0&&(b[Te]=null,D[Te].disconnect(ie))}for(let fe=0;fe<Y.added.length;fe++){let ie=Y.added[fe],Te=b.indexOf(ie);if(Te===-1){for(let Ie=0;Ie<D.length;Ie++)if(Ie>=b.length){b.push(ie),Te=Ie;break}else if(b[Ie]===null){b[Ie]=ie,Te=Ie;break}if(Te===-1)break}let De=D[Te];De&&De.connect(ie)}}let K=new V,Q=new V;function le(Y,fe,ie){K.setFromMatrixPosition(fe.matrixWorld),Q.setFromMatrixPosition(ie.matrixWorld);let Te=K.distanceTo(Q),De=fe.projectionMatrix.elements,Ie=ie.projectionMatrix.elements,pt=De[14]/(De[10]-1),Ve=De[14]/(De[10]+1),it=(De[9]+1)/De[5],ht=(De[9]-1)/De[5],Be=(De[8]-1)/De[0],wt=(Ie[8]+1)/Ie[0],mt=pt*Be,en=pt*wt,R=Te/(-Be+wt),It=R*-Be;if(fe.matrixWorld.decompose(Y.position,Y.quaternion,Y.scale),Y.translateX(It),Y.translateZ(R),Y.matrixWorld.compose(Y.position,Y.quaternion,Y.scale),Y.matrixWorldInverse.copy(Y.matrixWorld).invert(),De[10]===-1)Y.projectionMatrix.copy(fe.projectionMatrix),Y.projectionMatrixInverse.copy(fe.projectionMatrixInverse);else{let He=pt+R,ut=Ve+R,ae=mt-It,gt=en+(Te-It),x=it*Ve/ut*He,g=ht*Ve/ut*He;Y.projectionMatrix.makePerspective(ae,gt,x,g,He,ut),Y.projectionMatrixInverse.copy(Y.projectionMatrix).invert()}}function ye(Y,fe){fe===null?Y.matrixWorld.copy(Y.matrix):Y.matrixWorld.multiplyMatrices(fe.matrixWorld,Y.matrix),Y.matrixWorldInverse.copy(Y.matrixWorld).invert()}this.updateCamera=function(Y){if(r===null)return;let fe=Y.near,ie=Y.far;m.texture!==null&&(m.depthNear>0&&(fe=m.depthNear),m.depthFar>0&&(ie=m.depthFar)),k.near=L.near=w.near=fe,k.far=L.far=w.far=ie,(W!==k.near||X!==k.far)&&(r.updateRenderState({depthNear:k.near,depthFar:k.far}),W=k.near,X=k.far),k.layers.mask=Y.layers.mask|6,w.layers.mask=k.layers.mask&-5,L.layers.mask=k.layers.mask&-3;let Te=Y.parent,De=k.cameras;ye(k,Te);for(let Ie=0;Ie<De.length;Ie++)ye(De[Ie],Te);De.length===2?le(k,w,L):k.projectionMatrix.copy(w.projectionMatrix),Se(Y,k,Te)};function Se(Y,fe,ie){ie===null?Y.matrix.copy(fe.matrixWorld):(Y.matrix.copy(ie.matrixWorld),Y.matrix.invert(),Y.matrix.multiply(fe.matrixWorld)),Y.matrix.decompose(Y.position,Y.quaternion,Y.scale),Y.updateMatrixWorld(!0),Y.projectionMatrix.copy(fe.projectionMatrix),Y.projectionMatrixInverse.copy(fe.projectionMatrixInverse),Y.isPerspectiveCamera&&(Y.fov=sl*2*Math.atan(1/Y.projectionMatrix.elements[5]),Y.zoom=1)}this.getCamera=function(){return k},this.getFoveation=function(){if(!(d===null&&h===null))return c},this.setFoveation=function(Y){c=Y,d!==null&&(d.fixedFoveation=Y),h!==null&&h.fixedFoveation!==void 0&&(h.fixedFoveation=Y)},this.hasDepthSensing=function(){return m.texture!==null},this.getDepthSensingMesh=function(){return m.getMesh(k)},this.getCameraTexture=function(Y){return p[Y]};let qe=null;function nt(Y,fe){if(u=fe.getViewerPose(l||o),v=fe,u!==null){let ie=u.views;h!==null&&(e.setRenderTargetFramebuffer(E,h.framebuffer),e.setRenderTarget(E));let Te=!1;ie.length!==k.cameras.length&&(k.cameras.length=0,Te=!0);for(let Ve=0;Ve<ie.length;Ve++){let it=ie[Ve],ht=null;if(h!==null)ht=h.getViewport(it);else{let wt=f.getViewSubImage(d,it);ht=wt.viewport,Ve===0&&(e.setRenderTargetTextures(E,wt.colorTexture,wt.depthStencilTexture),e.setRenderTarget(E))}let Be=I[Ve];Be===void 0&&(Be=new Kt,Be.layers.enable(Ve),Be.viewport=new _t,I[Ve]=Be),Be.matrix.fromArray(it.transform.matrix),Be.matrix.decompose(Be.position,Be.quaternion,Be.scale),Be.projectionMatrix.fromArray(it.projectionMatrix),Be.projectionMatrixInverse.copy(Be.projectionMatrix).invert(),Be.viewport.set(ht.x,ht.y,ht.width,ht.height),Ve===0&&(k.matrix.copy(Be.matrix),k.matrix.decompose(k.position,k.quaternion,k.scale)),Te===!0&&k.cameras.push(Be)}let De=r.enabledFeatures;if(De&&De.includes("depth-sensing")&&r.depthUsage=="gpu-optimized"&&M){f=i.getBinding();let Ve=f.getDepthInformation(ie[0]);Ve&&Ve.isValid&&Ve.texture&&m.init(Ve,r.renderState)}if(De&&De.includes("camera-access")&&M){e.state.unbindTexture(),f=i.getBinding();for(let Ve=0;Ve<ie.length;Ve++){let it=ie[Ve].camera;if(it){let ht=p[it];ht||(ht=new Wo,p[it]=ht);let Be=f.getCameraImage(it);ht.sourceTexture=Be}}}}for(let ie=0;ie<D.length;ie++){let Te=b[ie],De=D[ie];Te!==null&&De!==void 0&&De.update(Te,fe,l||o)}qe&&qe(Y,fe),fe.detectedPlanes&&i.dispatchEvent({type:"planesdetected",data:fe}),v=null}let Oe=new k_;Oe.setAnimationLoop(nt),this.setAnimationLoop=function(Y){qe=Y},this.dispose=function(){}}},gA=new Tt,G_=new Ae;G_.set(-1,0,0,0,1,0,0,0,1);function vA(n,e){function t(m,p){m.matrixAutoUpdate===!0&&m.updateMatrix(),p.value.copy(m.matrix)}function i(m,p){p.color.getRGB(m.fogColor.value,cp(n)),p.isFog?(m.fogNear.value=p.near,m.fogFar.value=p.far):p.isFogExp2&&(m.fogDensity.value=p.density)}function r(m,p,S,T,E){p.isNodeMaterial?p.uniformsNeedUpdate=!1:p.isMeshBasicMaterial?s(m,p):p.isMeshLambertMaterial?(s(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshToonMaterial?(s(m,p),f(m,p)):p.isMeshPhongMaterial?(s(m,p),u(m,p),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)):p.isMeshStandardMaterial?(s(m,p),d(m,p),p.isMeshPhysicalMaterial&&h(m,p,E)):p.isMeshMatcapMaterial?(s(m,p),v(m,p)):p.isMeshDepthMaterial?s(m,p):p.isMeshDistanceMaterial?(s(m,p),M(m,p)):p.isMeshNormalMaterial?s(m,p):p.isLineBasicMaterial?(o(m,p),p.isLineDashedMaterial&&a(m,p)):p.isPointsMaterial?c(m,p,S,T):p.isSpriteMaterial?l(m,p):p.isShadowMaterial?(m.color.value.copy(p.color),m.opacity.value=p.opacity):p.isShaderMaterial&&(p.uniformsNeedUpdate=!1)}function s(m,p){m.opacity.value=p.opacity,p.color&&m.diffuse.value.copy(p.color),p.emissive&&m.emissive.value.copy(p.emissive).multiplyScalar(p.emissiveIntensity),p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.bumpMap&&(m.bumpMap.value=p.bumpMap,t(p.bumpMap,m.bumpMapTransform),m.bumpScale.value=p.bumpScale,p.side===qt&&(m.bumpScale.value*=-1)),p.normalMap&&(m.normalMap.value=p.normalMap,t(p.normalMap,m.normalMapTransform),m.normalScale.value.copy(p.normalScale),p.side===qt&&m.normalScale.value.negate()),p.displacementMap&&(m.displacementMap.value=p.displacementMap,t(p.displacementMap,m.displacementMapTransform),m.displacementScale.value=p.displacementScale,m.displacementBias.value=p.displacementBias),p.emissiveMap&&(m.emissiveMap.value=p.emissiveMap,t(p.emissiveMap,m.emissiveMapTransform)),p.specularMap&&(m.specularMap.value=p.specularMap,t(p.specularMap,m.specularMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest);let S=e.get(p),T=S.envMap,E=S.envMapRotation;T&&(m.envMap.value=T,m.envMapRotation.value.setFromMatrix4(gA.makeRotationFromEuler(E)).transpose(),T.isCubeTexture&&T.isRenderTargetTexture===!1&&m.envMapRotation.value.premultiply(G_),m.reflectivity.value=p.reflectivity,m.ior.value=p.ior,m.refractionRatio.value=p.refractionRatio),p.lightMap&&(m.lightMap.value=p.lightMap,m.lightMapIntensity.value=p.lightMapIntensity,t(p.lightMap,m.lightMapTransform)),p.aoMap&&(m.aoMap.value=p.aoMap,m.aoMapIntensity.value=p.aoMapIntensity,t(p.aoMap,m.aoMapTransform))}function o(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform))}function a(m,p){m.dashSize.value=p.dashSize,m.totalSize.value=p.dashSize+p.gapSize,m.scale.value=p.scale}function c(m,p,S,T){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.size.value=p.size*S,m.scale.value=T*.5,p.map&&(m.map.value=p.map,t(p.map,m.uvTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function l(m,p){m.diffuse.value.copy(p.color),m.opacity.value=p.opacity,m.rotation.value=p.rotation,p.map&&(m.map.value=p.map,t(p.map,m.mapTransform)),p.alphaMap&&(m.alphaMap.value=p.alphaMap,t(p.alphaMap,m.alphaMapTransform)),p.alphaTest>0&&(m.alphaTest.value=p.alphaTest)}function u(m,p){m.specular.value.copy(p.specular),m.shininess.value=Math.max(p.shininess,1e-4)}function f(m,p){p.gradientMap&&(m.gradientMap.value=p.gradientMap)}function d(m,p){m.metalness.value=p.metalness,p.metalnessMap&&(m.metalnessMap.value=p.metalnessMap,t(p.metalnessMap,m.metalnessMapTransform)),m.roughness.value=p.roughness,p.roughnessMap&&(m.roughnessMap.value=p.roughnessMap,t(p.roughnessMap,m.roughnessMapTransform)),p.envMap&&(m.envMapIntensity.value=p.envMapIntensity)}function h(m,p,S){m.ior.value=p.ior,p.sheen>0&&(m.sheenColor.value.copy(p.sheenColor).multiplyScalar(p.sheen),m.sheenRoughness.value=p.sheenRoughness,p.sheenColorMap&&(m.sheenColorMap.value=p.sheenColorMap,t(p.sheenColorMap,m.sheenColorMapTransform)),p.sheenRoughnessMap&&(m.sheenRoughnessMap.value=p.sheenRoughnessMap,t(p.sheenRoughnessMap,m.sheenRoughnessMapTransform))),p.clearcoat>0&&(m.clearcoat.value=p.clearcoat,m.clearcoatRoughness.value=p.clearcoatRoughness,p.clearcoatMap&&(m.clearcoatMap.value=p.clearcoatMap,t(p.clearcoatMap,m.clearcoatMapTransform)),p.clearcoatRoughnessMap&&(m.clearcoatRoughnessMap.value=p.clearcoatRoughnessMap,t(p.clearcoatRoughnessMap,m.clearcoatRoughnessMapTransform)),p.clearcoatNormalMap&&(m.clearcoatNormalMap.value=p.clearcoatNormalMap,t(p.clearcoatNormalMap,m.clearcoatNormalMapTransform),m.clearcoatNormalScale.value.copy(p.clearcoatNormalScale),p.side===qt&&m.clearcoatNormalScale.value.negate())),p.dispersion>0&&(m.dispersion.value=p.dispersion),p.iridescence>0&&(m.iridescence.value=p.iridescence,m.iridescenceIOR.value=p.iridescenceIOR,m.iridescenceThicknessMinimum.value=p.iridescenceThicknessRange[0],m.iridescenceThicknessMaximum.value=p.iridescenceThicknessRange[1],p.iridescenceMap&&(m.iridescenceMap.value=p.iridescenceMap,t(p.iridescenceMap,m.iridescenceMapTransform)),p.iridescenceThicknessMap&&(m.iridescenceThicknessMap.value=p.iridescenceThicknessMap,t(p.iridescenceThicknessMap,m.iridescenceThicknessMapTransform))),p.transmission>0&&(m.transmission.value=p.transmission,m.transmissionSamplerMap.value=S.texture,m.transmissionSamplerSize.value.set(S.width,S.height),p.transmissionMap&&(m.transmissionMap.value=p.transmissionMap,t(p.transmissionMap,m.transmissionMapTransform)),m.thickness.value=p.thickness,p.thicknessMap&&(m.thicknessMap.value=p.thicknessMap,t(p.thicknessMap,m.thicknessMapTransform)),m.attenuationDistance.value=p.attenuationDistance,m.attenuationColor.value.copy(p.attenuationColor)),p.anisotropy>0&&(m.anisotropyVector.value.set(p.anisotropy*Math.cos(p.anisotropyRotation),p.anisotropy*Math.sin(p.anisotropyRotation)),p.anisotropyMap&&(m.anisotropyMap.value=p.anisotropyMap,t(p.anisotropyMap,m.anisotropyMapTransform))),m.specularIntensity.value=p.specularIntensity,m.specularColor.value.copy(p.specularColor),p.specularColorMap&&(m.specularColorMap.value=p.specularColorMap,t(p.specularColorMap,m.specularColorMapTransform)),p.specularIntensityMap&&(m.specularIntensityMap.value=p.specularIntensityMap,t(p.specularIntensityMap,m.specularIntensityMapTransform))}function v(m,p){p.matcap&&(m.matcap.value=p.matcap)}function M(m,p){let S=e.get(p).light;m.referencePosition.value.setFromMatrixPosition(S.matrixWorld),m.nearDistance.value=S.shadow.camera.near,m.farDistance.value=S.shadow.camera.far}return{refreshFogUniforms:i,refreshMaterialUniforms:r}}function yA(n,e,t,i){let r={},s={},o=[],a=n.getParameter(n.MAX_UNIFORM_BUFFER_BINDINGS);function c(S,T){let E=T.program;i.uniformBlockBinding(S,E)}function l(S,T){let E=r[S.id];E===void 0&&(v(S),E=u(S),r[S.id]=E,S.addEventListener("dispose",m));let D=T.program;i.updateUBOMapping(S,D);let b=e.render.frame;s[S.id]!==b&&(d(S),s[S.id]=b)}function u(S){let T=f();S.__bindingPointIndex=T;let E=n.createBuffer(),D=S.__size,b=S.usage;return n.bindBuffer(n.UNIFORM_BUFFER,E),n.bufferData(n.UNIFORM_BUFFER,D,b),n.bindBuffer(n.UNIFORM_BUFFER,null),n.bindBufferBase(n.UNIFORM_BUFFER,T,E),E}function f(){for(let S=0;S<a;S++)if(o.indexOf(S)===-1)return o.push(S),S;return we("WebGLRenderer: Maximum number of simultaneously usable uniforms groups reached."),0}function d(S){let T=r[S.id],E=S.uniforms,D=S.__cache;n.bindBuffer(n.UNIFORM_BUFFER,T);for(let b=0,A=E.length;b<A;b++){let y=Array.isArray(E[b])?E[b]:[E[b]];for(let w=0,L=y.length;w<L;w++){let I=y[w];if(h(I,b,w,D)===!0){let k=I.__offset,W=Array.isArray(I.value)?I.value:[I.value],X=0;for(let P=0;P<W.length;P++){let z=W[P],F=M(z);typeof z=="number"||typeof z=="boolean"?(I.__data[0]=z,n.bufferSubData(n.UNIFORM_BUFFER,k+X,I.__data)):z.isMatrix3?(I.__data[0]=z.elements[0],I.__data[1]=z.elements[1],I.__data[2]=z.elements[2],I.__data[3]=0,I.__data[4]=z.elements[3],I.__data[5]=z.elements[4],I.__data[6]=z.elements[5],I.__data[7]=0,I.__data[8]=z.elements[6],I.__data[9]=z.elements[7],I.__data[10]=z.elements[8],I.__data[11]=0):ArrayBuffer.isView(z)?I.__data.set(new z.constructor(z.buffer,z.byteOffset,I.__data.length)):(z.toArray(I.__data,X),X+=F.storage/Float32Array.BYTES_PER_ELEMENT)}n.bufferSubData(n.UNIFORM_BUFFER,k,I.__data)}}}n.bindBuffer(n.UNIFORM_BUFFER,null)}function h(S,T,E,D){let b=S.value,A=T+"_"+E;if(D[A]===void 0)return typeof b=="number"||typeof b=="boolean"?D[A]=b:ArrayBuffer.isView(b)?D[A]=b.slice():D[A]=b.clone(),!0;{let y=D[A];if(typeof b=="number"||typeof b=="boolean"){if(y!==b)return D[A]=b,!0}else{if(ArrayBuffer.isView(b))return!0;if(y.equals(b)===!1)return y.copy(b),!0}}return!1}function v(S){let T=S.uniforms,E=0,D=16;for(let A=0,y=T.length;A<y;A++){let w=Array.isArray(T[A])?T[A]:[T[A]];for(let L=0,I=w.length;L<I;L++){let k=w[L],W=Array.isArray(k.value)?k.value:[k.value];for(let X=0,P=W.length;X<P;X++){let z=W[X],F=M(z),K=E%D,Q=K%F.boundary,le=K+Q;E+=Q,le!==0&&D-le<F.storage&&(E+=D-le),k.__data=new Float32Array(F.storage/Float32Array.BYTES_PER_ELEMENT),k.__offset=E,E+=F.storage}}}let b=E%D;return b>0&&(E+=D-b),S.__size=E,S.__cache={},this}function M(S){let T={boundary:0,storage:0};return typeof S=="number"||typeof S=="boolean"?(T.boundary=4,T.storage=4):S.isVector2?(T.boundary=8,T.storage=8):S.isVector3||S.isColor?(T.boundary=16,T.storage=12):S.isVector4?(T.boundary=16,T.storage=16):S.isMatrix3?(T.boundary=48,T.storage=48):S.isMatrix4?(T.boundary=64,T.storage=64):S.isTexture?be("WebGLRenderer: Texture samplers can not be part of an uniforms group."):ArrayBuffer.isView(S)?(T.boundary=16,T.storage=S.byteLength):be("WebGLRenderer: Unsupported uniform value type.",S),T}function m(S){let T=S.target;T.removeEventListener("dispose",m);let E=o.indexOf(T.__bindingPointIndex);o.splice(E,1),n.deleteBuffer(r[T.id]),delete r[T.id],delete s[T.id]}function p(){for(let S in r)n.deleteBuffer(r[S]);o=[],r={},s={}}return{bind:c,update:l,dispose:p}}var _A=new Uint16Array([12469,15057,12620,14925,13266,14620,13807,14376,14323,13990,14545,13625,14713,13328,14840,12882,14931,12528,14996,12233,15039,11829,15066,11525,15080,11295,15085,10976,15082,10705,15073,10495,13880,14564,13898,14542,13977,14430,14158,14124,14393,13732,14556,13410,14702,12996,14814,12596,14891,12291,14937,11834,14957,11489,14958,11194,14943,10803,14921,10506,14893,10278,14858,9960,14484,14039,14487,14025,14499,13941,14524,13740,14574,13468,14654,13106,14743,12678,14818,12344,14867,11893,14889,11509,14893,11180,14881,10751,14852,10428,14812,10128,14765,9754,14712,9466,14764,13480,14764,13475,14766,13440,14766,13347,14769,13070,14786,12713,14816,12387,14844,11957,14860,11549,14868,11215,14855,10751,14825,10403,14782,10044,14729,9651,14666,9352,14599,9029,14967,12835,14966,12831,14963,12804,14954,12723,14936,12564,14917,12347,14900,11958,14886,11569,14878,11247,14859,10765,14828,10401,14784,10011,14727,9600,14660,9289,14586,8893,14508,8533,15111,12234,15110,12234,15104,12216,15092,12156,15067,12010,15028,11776,14981,11500,14942,11205,14902,10752,14861,10393,14812,9991,14752,9570,14682,9252,14603,8808,14519,8445,14431,8145,15209,11449,15208,11451,15202,11451,15190,11438,15163,11384,15117,11274,15055,10979,14994,10648,14932,10343,14871,9936,14803,9532,14729,9218,14645,8742,14556,8381,14461,8020,14365,7603,15273,10603,15272,10607,15267,10619,15256,10631,15231,10614,15182,10535,15118,10389,15042,10167,14963,9787,14883,9447,14800,9115,14710,8665,14615,8318,14514,7911,14411,7507,14279,7198,15314,9675,15313,9683,15309,9712,15298,9759,15277,9797,15229,9773,15166,9668,15084,9487,14995,9274,14898,8910,14800,8539,14697,8234,14590,7790,14479,7409,14367,7067,14178,6621,15337,8619,15337,8631,15333,8677,15325,8769,15305,8871,15264,8940,15202,8909,15119,8775,15022,8565,14916,8328,14804,8009,14688,7614,14569,7287,14448,6888,14321,6483,14088,6171,15350,7402,15350,7419,15347,7480,15340,7613,15322,7804,15287,7973,15229,8057,15148,8012,15046,7846,14933,7611,14810,7357,14682,7069,14552,6656,14421,6316,14251,5948,14007,5528,15356,5942,15356,5977,15353,6119,15348,6294,15332,6551,15302,6824,15249,7044,15171,7122,15070,7050,14949,6861,14818,6611,14679,6349,14538,6067,14398,5651,14189,5311,13935,4958,15359,4123,15359,4153,15356,4296,15353,4646,15338,5160,15311,5508,15263,5829,15188,6042,15088,6094,14966,6001,14826,5796,14678,5543,14527,5287,14377,4985,14133,4586,13869,4257,15360,1563,15360,1642,15358,2076,15354,2636,15341,3350,15317,4019,15273,4429,15203,4732,15105,4911,14981,4932,14836,4818,14679,4621,14517,4386,14359,4156,14083,3795,13808,3437,15360,122,15360,137,15358,285,15355,636,15344,1274,15322,2177,15281,2765,15215,3223,15120,3451,14995,3569,14846,3567,14681,3466,14511,3305,14344,3121,14037,2800,13753,2467,15360,0,15360,1,15359,21,15355,89,15346,253,15325,479,15287,796,15225,1148,15133,1492,15008,1749,14856,1882,14685,1886,14506,1783,14324,1608,13996,1398,13702,1183]),ti=null;function xA(){return ti===null&&(ti=new ul(_A,16,16,Xi,ei),ti.name="DFG_LUT",ti.minFilter=Ot,ti.magFilter=Ot,ti.wrapS=Yn,ti.wrapT=Yn,ti.generateMipmaps=!1,ti.needsUpdate=!0),ti}var mu=class{constructor(e={}){let{canvas:t=l_(),context:i=null,depth:r=!0,stencil:s=!1,alpha:o=!1,antialias:a=!1,premultipliedAlpha:c=!0,preserveDrawingBuffer:l=!1,powerPreference:u="default",failIfMajorPerformanceCaveat:f=!1,reversedDepthBuffer:d=!1,outputBufferType:h=pn}=e;this.isWebGLRenderer=!0;let v;if(i!==null){if(typeof WebGLRenderingContext<"u"&&i instanceof WebGLRenderingContext)throw new Error("THREE.WebGLRenderer: WebGL 1 is not supported since r163.");v=i.getContextAttributes().alpha}else v=o;let M=h,m=new Set([Nl,Rl,Dl]),p=new Set([pn,kn,Ls,Os,Cl,Al]),S=new Uint32Array(4),T=new Int32Array(4),E=new V,D=null,b=null,A=[],y=[],w=null;this.domElement=t,this.debug={checkShaderErrors:!0,onShaderError:null},this.autoClear=!0,this.autoClearColor=!0,this.autoClearDepth=!0,this.autoClearStencil=!0,this.sortObjects=!0,this.clippingPlanes=[],this.localClippingEnabled=!1,this.toneMapping=Fn,this.toneMappingExposure=1,this.transmissionResolutionScale=1;let L=this,I=!1,k=null;this._outputColorSpace=un;let W=0,X=0,P=null,z=-1,F=null,K=new _t,Q=new _t,le=null,ye=new Ze(0),Se=0,qe=t.width,nt=t.height,Oe=1,Y=null,fe=null,ie=new _t(0,0,qe,nt),Te=new _t(0,0,qe,nt),De=!1,Ie=new zo,pt=!1,Ve=!1,it=new Tt,ht=new V,Be=new _t,wt={background:null,fog:null,environment:null,overrideMaterial:null,isScene:!0},mt=!1;function en(){return P===null?Oe:1}let R=i;function It(_,N){return t.getContext(_,N)}try{let _={alpha:!0,depth:r,stencil:s,antialias:a,premultipliedAlpha:c,preserveDrawingBuffer:l,powerPreference:u,failIfMajorPerformanceCaveat:f};if("setAttribute"in t&&t.setAttribute("data-engine",`three.js r${"184"}`),t.addEventListener("webglcontextlost",Z,!1),t.addEventListener("webglcontextrestored",xe,!1),t.addEventListener("webglcontextcreationerror",Ne,!1),R===null){let N="webgl2";if(R=It(N,_),R===null)throw It(N)?new Error("Error creating WebGL context with your selected attributes."):new Error("Error creating WebGL context.")}}catch(_){throw we("WebGLRenderer: "+_.message),_}let He,ut,ae,gt,x,g,O,$,J,ee,oe,G,q,he,ge,re,te,Ce,Fe,Je,C,ne,j;function pe(){He=new II(R),He.init(),C=new hA(R,He),ut=new _I(R,He,e,C),ae=new dA(R,He),ut.reversedDepthBuffer&&d&&ae.buffers.depth.setReversed(!0),gt=new DI(R),x=new JC,g=new fA(R,He,ae,x,ut,C,gt),O=new wI(L),$=new Lb(R),ne=new vI(R,$),J=new CI(R,$,gt,ne),ee=new NI(R,J,$,ne,gt),Ce=new RI(R,ut,g),ge=new xI(x),oe=new ZC(L,O,He,ut,ne,ge),G=new vA(L,x),q=new QC,he=new sA(He),te=new gI(L,O,ae,ee,v,c),re=new uA(L,ee,ut),j=new yA(R,gt,ut,ae),Fe=new yI(R,He,gt),Je=new AI(R,He,gt),gt.programs=oe.programs,L.capabilities=ut,L.extensions=He,L.properties=x,L.renderLists=q,L.shadowMap=re,L.state=ae,L.info=gt}pe(),M!==pn&&(w=new LI(M,t.width,t.height,r,s));let se=new wp(L,R);this.xr=se,this.getContext=function(){return R},this.getContextAttributes=function(){return R.getContextAttributes()},this.forceContextLoss=function(){let _=He.get("WEBGL_lose_context");_&&_.loseContext()},this.forceContextRestore=function(){let _=He.get("WEBGL_lose_context");_&&_.restoreContext()},this.getPixelRatio=function(){return Oe},this.setPixelRatio=function(_){_!==void 0&&(Oe=_,this.setSize(qe,nt,!1))},this.getSize=function(_){return _.set(qe,nt)},this.setSize=function(_,N,H=!0){if(se.isPresenting){be("WebGLRenderer: Can't change size while VR device is presenting.");return}qe=_,nt=N,t.width=Math.floor(_*Oe),t.height=Math.floor(N*Oe),H===!0&&(t.style.width=_+"px",t.style.height=N+"px"),w!==null&&w.setSize(t.width,t.height),this.setViewport(0,0,_,N)},this.getDrawingBufferSize=function(_){return _.set(qe*Oe,nt*Oe).floor()},this.setDrawingBufferSize=function(_,N,H){qe=_,nt=N,Oe=H,t.width=Math.floor(_*H),t.height=Math.floor(N*H),this.setViewport(0,0,_,N)},this.setEffects=function(_){if(M===pn){we("THREE.WebGLRenderer: setEffects() requires outputBufferType set to HalfFloatType or FloatType.");return}if(_){for(let N=0;N<_.length;N++)if(_[N].isOutputPass===!0){be("THREE.WebGLRenderer: OutputPass is not needed in setEffects(). Tone mapping and color space conversion are applied automatically.");break}}w.setEffects(_||[])},this.getCurrentViewport=function(_){return _.copy(K)},this.getViewport=function(_){return _.copy(ie)},this.setViewport=function(_,N,H,U){_.isVector4?ie.set(_.x,_.y,_.z,_.w):ie.set(_,N,H,U),ae.viewport(K.copy(ie).multiplyScalar(Oe).round())},this.getScissor=function(_){return _.copy(Te)},this.setScissor=function(_,N,H,U){_.isVector4?Te.set(_.x,_.y,_.z,_.w):Te.set(_,N,H,U),ae.scissor(Q.copy(Te).multiplyScalar(Oe).round())},this.getScissorTest=function(){return De},this.setScissorTest=function(_){ae.setScissorTest(De=_)},this.setOpaqueSort=function(_){Y=_},this.setTransparentSort=function(_){fe=_},this.getClearColor=function(_){return _.copy(te.getClearColor())},this.setClearColor=function(){te.setClearColor(...arguments)},this.getClearAlpha=function(){return te.getClearAlpha()},this.setClearAlpha=function(){te.setClearAlpha(...arguments)},this.clear=function(_=!0,N=!0,H=!0){let U=0;if(_){let B=!1;if(P!==null){let de=P.texture.format;B=m.has(de)}if(B){let de=P.texture.type,ve=p.has(de),ue=te.getClearColor(),_e=te.getClearAlpha(),Me=ue.r,Pe=ue.g,Ue=ue.b;ve?(S[0]=Me,S[1]=Pe,S[2]=Ue,S[3]=_e,R.clearBufferuiv(R.COLOR,0,S)):(T[0]=Me,T[1]=Pe,T[2]=Ue,T[3]=_e,R.clearBufferiv(R.COLOR,0,T))}else U|=R.COLOR_BUFFER_BIT}N&&(U|=R.DEPTH_BUFFER_BIT,this.state.buffers.depth.setMask(!0)),H&&(U|=R.STENCIL_BUFFER_BIT,this.state.buffers.stencil.setMask(4294967295)),U!==0&&R.clear(U)},this.clearColor=function(){this.clear(!0,!1,!1)},this.clearDepth=function(){this.clear(!1,!0,!1)},this.clearStencil=function(){this.clear(!1,!1,!0)},this.setNodesHandler=function(_){_.setRenderer(this),k=_},this.dispose=function(){t.removeEventListener("webglcontextlost",Z,!1),t.removeEventListener("webglcontextrestored",xe,!1),t.removeEventListener("webglcontextcreationerror",Ne,!1),te.dispose(),q.dispose(),he.dispose(),x.dispose(),O.dispose(),ee.dispose(),ne.dispose(),j.dispose(),oe.dispose(),se.dispose(),se.removeEventListener("sessionstart",Ip),se.removeEventListener("sessionend",Cp),Zi.stop()};function Z(_){_.preventDefault(),ap("WebGLRenderer: Context Lost."),I=!0}function xe(){ap("WebGLRenderer: Context Restored."),I=!1;let _=gt.autoReset,N=re.enabled,H=re.autoUpdate,U=re.needsUpdate,B=re.type;pe(),gt.autoReset=_,re.enabled=N,re.autoUpdate=H,re.needsUpdate=U,re.type=B}function Ne(_){we("WebGLRenderer: A WebGL context could not be created. Reason: ",_.statusMessage)}function xt(_){let N=_.target;N.removeEventListener("dispose",xt),rt(N)}function rt(_){ii(_),x.remove(_)}function ii(_){let N=x.get(_).programs;N!==void 0&&(N.forEach(function(H){oe.releaseProgram(H)}),_.isShaderMaterial&&oe.releaseShaderCache(_))}this.renderBufferDirect=function(_,N,H,U,B,de){N===null&&(N=wt);let ve=B.isMesh&&B.matrixWorld.determinant()<0,ue=q_(_,N,H,U,B);ae.setMaterial(U,ve);let _e=H.index,Me=1;if(U.wireframe===!0){if(_e=J.getWireframeAttribute(H),_e===void 0)return;Me=2}let Pe=H.drawRange,Ue=H.attributes.position,Ee=Pe.start*Me,st=(Pe.start+Pe.count)*Me;de!==null&&(Ee=Math.max(Ee,de.start*Me),st=Math.min(st,(de.start+de.count)*Me)),_e!==null?(Ee=Math.max(Ee,0),st=Math.min(st,_e.count)):Ue!=null&&(Ee=Math.max(Ee,0),st=Math.min(st,Ue.count));let Mt=st-Ee;if(Mt<0||Mt===1/0)return;ne.setup(B,U,ue,H,_e);let vt,ct=Fe;if(_e!==null&&(vt=$.get(_e),ct=Je,ct.setIndex(vt)),B.isMesh)U.wireframe===!0?(ae.setLineWidth(U.wireframeLinewidth*en()),ct.setMode(R.LINES)):ct.setMode(R.TRIANGLES);else if(B.isLine){let Ft=U.linewidth;Ft===void 0&&(Ft=1),ae.setLineWidth(Ft*en()),B.isLineSegments?ct.setMode(R.LINES):B.isLineLoop?ct.setMode(R.LINE_LOOP):ct.setMode(R.LINE_STRIP)}else B.isPoints?ct.setMode(R.POINTS):B.isSprite&&ct.setMode(R.TRIANGLES);if(B.isBatchedMesh)if(He.get("WEBGL_multi_draw"))ct.renderMultiDraw(B._multiDrawStarts,B._multiDrawCounts,B._multiDrawCount);else{let Ft=B._multiDrawStarts,me=B._multiDrawCounts,tn=B._multiDrawCount,Xe=_e?$.get(_e).bytesPerElement:1,mn=x.get(U).currentProgram.getUniforms();for(let Vn=0;Vn<tn;Vn++)mn.setValue(R,"_gl_DrawID",Vn),ct.render(Ft[Vn]/Xe,me[Vn])}else if(B.isInstancedMesh)ct.renderInstances(Ee,Mt,B.count);else if(H.isInstancedBufferGeometry){let Ft=H._maxInstanceCount!==void 0?H._maxInstanceCount:1/0,me=Math.min(H.instanceCount,Ft);ct.renderInstances(Ee,Mt,me)}else ct.render(Ee,Mt)};function Bn(_,N,H){_.transparent===!0&&_.side===En&&_.forceSinglePass===!1?(_.side=qt,_.needsUpdate=!0,ca(_,N,H),_.side=gi,_.needsUpdate=!0,ca(_,N,H),_.side=En):ca(_,N,H)}this.compile=function(_,N,H=null){H===null&&(H=_),b=he.get(H),b.init(N),y.push(b),H.traverseVisible(function(B){B.isLight&&B.layers.test(N.layers)&&(b.pushLight(B),B.castShadow&&b.pushShadow(B))}),_!==H&&_.traverseVisible(function(B){B.isLight&&B.layers.test(N.layers)&&(b.pushLight(B),B.castShadow&&b.pushShadow(B))}),b.setupLights();let U=new Set;return _.traverse(function(B){if(!(B.isMesh||B.isPoints||B.isLine||B.isSprite))return;let de=B.material;if(de)if(Array.isArray(de))for(let ve=0;ve<de.length;ve++){let ue=de[ve];Bn(ue,H,B),U.add(ue)}else Bn(de,H,B),U.add(de)}),b=y.pop(),U},this.compileAsync=function(_,N,H=null){let U=this.compile(_,N,H);return new Promise(B=>{function de(){if(U.forEach(function(ve){x.get(ve).currentProgram.isReady()&&U.delete(ve)}),U.size===0){B(_);return}setTimeout(de,10)}He.get("KHR_parallel_shader_compile")!==null?de():setTimeout(de,10)})};let Mu=null;function j_(_){Mu&&Mu(_)}function Ip(){Zi.stop()}function Cp(){Zi.start()}let Zi=new k_;Zi.setAnimationLoop(j_),typeof self<"u"&&Zi.setContext(self),this.setAnimationLoop=function(_){Mu=_,se.setAnimationLoop(_),_===null?Zi.stop():Zi.start()},se.addEventListener("sessionstart",Ip),se.addEventListener("sessionend",Cp),this.render=function(_,N){if(N!==void 0&&N.isCamera!==!0){we("WebGLRenderer.render: camera is not an instance of THREE.Camera.");return}if(I===!0)return;k!==null&&k.renderStart(_,N);let H=se.enabled===!0&&se.isPresenting===!0,U=w!==null&&(P===null||H)&&w.begin(L,P);if(_.matrixWorldAutoUpdate===!0&&_.updateMatrixWorld(),N.parent===null&&N.matrixWorldAutoUpdate===!0&&N.updateMatrixWorld(),se.enabled===!0&&se.isPresenting===!0&&(w===null||w.isCompositing()===!1)&&(se.cameraAutoUpdate===!0&&se.updateCamera(N),N=se.getCamera()),_.isScene===!0&&_.onBeforeRender(L,_,N,P),b=he.get(_,y.length),b.init(N),b.state.textureUnits=g.getTextureUnits(),y.push(b),it.multiplyMatrices(N.projectionMatrix,N.matrixWorldInverse),Ie.setFromProjectionMatrix(it,On,N.reversedDepth),Ve=this.localClippingEnabled,pt=ge.init(this.clippingPlanes,Ve),D=q.get(_,A.length),D.init(),A.push(D),se.enabled===!0&&se.isPresenting===!0){let ve=L.xr.getDepthSensingMesh();ve!==null&&Su(ve,N,-1/0,L.sortObjects)}Su(_,N,0,L.sortObjects),D.finish(),L.sortObjects===!0&&D.sort(Y,fe),mt=se.enabled===!1||se.isPresenting===!1||se.hasDepthSensing()===!1,mt&&te.addToRenderList(D,_),this.info.render.frame++,pt===!0&&ge.beginShadows();let B=b.state.shadowsArray;if(re.render(B,_,N),pt===!0&&ge.endShadows(),this.info.autoReset===!0&&this.info.reset(),(U&&w.hasRenderPass())===!1){let ve=D.opaque,ue=D.transmissive;if(b.setupLights(),N.isArrayCamera){let _e=N.cameras;if(ue.length>0)for(let Me=0,Pe=_e.length;Me<Pe;Me++){let Ue=_e[Me];Dp(ve,ue,_,Ue)}mt&&te.render(_);for(let Me=0,Pe=_e.length;Me<Pe;Me++){let Ue=_e[Me];Ap(D,_,Ue,Ue.viewport)}}else ue.length>0&&Dp(ve,ue,_,N),mt&&te.render(_),Ap(D,_,N)}P!==null&&X===0&&(g.updateMultisampleRenderTarget(P),g.updateRenderTargetMipmap(P)),U&&w.end(L),_.isScene===!0&&_.onAfterRender(L,_,N),ne.resetDefaultState(),z=-1,F=null,y.pop(),y.length>0?(b=y[y.length-1],g.setTextureUnits(b.state.textureUnits),pt===!0&&ge.setGlobalState(L.clippingPlanes,b.state.camera)):b=null,A.pop(),A.length>0?D=A[A.length-1]:D=null,k!==null&&k.renderEnd()};function Su(_,N,H,U){if(_.visible===!1)return;if(_.layers.test(N.layers)){if(_.isGroup)H=_.renderOrder;else if(_.isLOD)_.autoUpdate===!0&&_.update(N);else if(_.isLightProbeGrid)b.pushLightProbeGrid(_);else if(_.isLight)b.pushLight(_),_.castShadow&&b.pushShadow(_);else if(_.isSprite){if(!_.frustumCulled||Ie.intersectsSprite(_)){U&&Be.setFromMatrixPosition(_.matrixWorld).applyMatrix4(it);let ve=ee.update(_),ue=_.material;ue.visible&&D.push(_,ve,ue,H,Be.z,null)}}else if((_.isMesh||_.isLine||_.isPoints)&&(!_.frustumCulled||Ie.intersectsObject(_))){let ve=ee.update(_),ue=_.material;if(U&&(_.boundingSphere!==void 0?(_.boundingSphere===null&&_.computeBoundingSphere(),Be.copy(_.boundingSphere.center)):(ve.boundingSphere===null&&ve.computeBoundingSphere(),Be.copy(ve.boundingSphere.center)),Be.applyMatrix4(_.matrixWorld).applyMatrix4(it)),Array.isArray(ue)){let _e=ve.groups;for(let Me=0,Pe=_e.length;Me<Pe;Me++){let Ue=_e[Me],Ee=ue[Ue.materialIndex];Ee&&Ee.visible&&D.push(_,ve,Ee,H,Be.z,Ue)}}else ue.visible&&D.push(_,ve,ue,H,Be.z,null)}}let de=_.children;for(let ve=0,ue=de.length;ve<ue;ve++)Su(de[ve],N,H,U)}function Ap(_,N,H,U){let{opaque:B,transmissive:de,transparent:ve}=_;b.setupLightsView(H),pt===!0&&ge.setGlobalState(L.clippingPlanes,H),U&&ae.viewport(K.copy(U)),B.length>0&&aa(B,N,H),de.length>0&&aa(de,N,H),ve.length>0&&aa(ve,N,H),ae.buffers.depth.setTest(!0),ae.buffers.depth.setMask(!0),ae.buffers.color.setMask(!0),ae.setPolygonOffset(!1)}function Dp(_,N,H,U){if((H.isScene===!0?H.overrideMaterial:null)!==null)return;if(b.state.transmissionRenderTarget[U.id]===void 0){let Ee=He.has("EXT_color_buffer_half_float")||He.has("EXT_color_buffer_float");b.state.transmissionRenderTarget[U.id]=new dn(1,1,{generateMipmaps:!0,type:Ee?ei:pn,minFilter:$i,samples:Math.max(4,ut.samples),stencilBuffer:s,resolveDepthBuffer:!1,resolveStencilBuffer:!1,colorSpace:ze.workingColorSpace})}let de=b.state.transmissionRenderTarget[U.id],ve=U.viewport||K;de.setSize(ve.z*L.transmissionResolutionScale,ve.w*L.transmissionResolutionScale);let ue=L.getRenderTarget(),_e=L.getActiveCubeFace(),Me=L.getActiveMipmapLevel();L.setRenderTarget(de),L.getClearColor(ye),Se=L.getClearAlpha(),Se<1&&L.setClearColor(16777215,.5),L.clear(),mt&&te.render(H);let Pe=L.toneMapping;L.toneMapping=Fn;let Ue=U.viewport;if(U.viewport!==void 0&&(U.viewport=void 0),b.setupLightsView(U),pt===!0&&ge.setGlobalState(L.clippingPlanes,U),aa(_,H,U),g.updateMultisampleRenderTarget(de),g.updateRenderTargetMipmap(de),He.has("WEBGL_multisampled_render_to_texture")===!1){let Ee=!1;for(let st=0,Mt=N.length;st<Mt;st++){let vt=N[st],{object:ct,geometry:Ft,material:me,group:tn}=vt;if(me.side===En&&ct.layers.test(U.layers)){let Xe=me.side;me.side=qt,me.needsUpdate=!0,Rp(ct,H,U,Ft,me,tn),me.side=Xe,me.needsUpdate=!0,Ee=!0}}Ee===!0&&(g.updateMultisampleRenderTarget(de),g.updateRenderTargetMipmap(de))}L.setRenderTarget(ue,_e,Me),L.setClearColor(ye,Se),Ue!==void 0&&(U.viewport=Ue),L.toneMapping=Pe}function aa(_,N,H){let U=N.isScene===!0?N.overrideMaterial:null;for(let B=0,de=_.length;B<de;B++){let ve=_[B],{object:ue,geometry:_e,group:Me}=ve,Pe=ve.material;Pe.allowOverride===!0&&U!==null&&(Pe=U),ue.layers.test(H.layers)&&Rp(ue,N,H,_e,Pe,Me)}}function Rp(_,N,H,U,B,de){_.onBeforeRender(L,N,H,U,B,de),_.modelViewMatrix.multiplyMatrices(H.matrixWorldInverse,_.matrixWorld),_.normalMatrix.getNormalMatrix(_.modelViewMatrix),B.onBeforeRender(L,N,H,U,_,de),B.transparent===!0&&B.side===En&&B.forceSinglePass===!1?(B.side=qt,B.needsUpdate=!0,L.renderBufferDirect(H,N,U,B,_,de),B.side=gi,B.needsUpdate=!0,L.renderBufferDirect(H,N,U,B,_,de),B.side=En):L.renderBufferDirect(H,N,U,B,_,de),_.onAfterRender(L,N,H,U,B,de)}function ca(_,N,H){N.isScene!==!0&&(N=wt);let U=x.get(_),B=b.state.lights,de=b.state.shadowsArray,ve=B.state.version,ue=oe.getParameters(_,B.state,de,N,H,b.state.lightProbeGridArray),_e=oe.getProgramCacheKey(ue),Me=U.programs;U.environment=_.isMeshStandardMaterial||_.isMeshLambertMaterial||_.isMeshPhongMaterial?N.environment:null,U.fog=N.fog;let Pe=_.isMeshStandardMaterial||_.isMeshLambertMaterial&&!_.envMap||_.isMeshPhongMaterial&&!_.envMap;U.envMap=O.get(_.envMap||U.environment,Pe),U.envMapRotation=U.environment!==null&&_.envMap===null?N.environmentRotation:_.envMapRotation,Me===void 0&&(_.addEventListener("dispose",xt),Me=new Map,U.programs=Me);let Ue=Me.get(_e);if(Ue!==void 0){if(U.currentProgram===Ue&&U.lightsStateVersion===ve)return Pp(_,ue),Ue}else ue.uniforms=oe.getUniforms(_),k!==null&&_.isNodeMaterial&&k.build(_,H,ue),_.onBeforeCompile(ue,L),Ue=oe.acquireProgram(ue,_e),Me.set(_e,Ue),U.uniforms=ue.uniforms;let Ee=U.uniforms;return(!_.isShaderMaterial&&!_.isRawShaderMaterial||_.clipping===!0)&&(Ee.clippingPlanes=ge.uniform),Pp(_,ue),U.needsLights=Y_(_),U.lightsStateVersion=ve,U.needsLights&&(Ee.ambientLightColor.value=B.state.ambient,Ee.lightProbe.value=B.state.probe,Ee.directionalLights.value=B.state.directional,Ee.directionalLightShadows.value=B.state.directionalShadow,Ee.spotLights.value=B.state.spot,Ee.spotLightShadows.value=B.state.spotShadow,Ee.rectAreaLights.value=B.state.rectArea,Ee.ltc_1.value=B.state.rectAreaLTC1,Ee.ltc_2.value=B.state.rectAreaLTC2,Ee.pointLights.value=B.state.point,Ee.pointLightShadows.value=B.state.pointShadow,Ee.hemisphereLights.value=B.state.hemi,Ee.directionalShadowMatrix.value=B.state.directionalShadowMatrix,Ee.spotLightMatrix.value=B.state.spotLightMatrix,Ee.spotLightMap.value=B.state.spotLightMap,Ee.pointShadowMatrix.value=B.state.pointShadowMatrix),U.lightProbeGrid=b.state.lightProbeGridArray.length>0,U.currentProgram=Ue,U.uniformsList=null,Ue}function Np(_){if(_.uniformsList===null){let N=_.currentProgram.getUniforms();_.uniformsList=ks.seqWithValue(N.seq,_.uniforms)}return _.uniformsList}function Pp(_,N){let H=x.get(_);H.outputColorSpace=N.outputColorSpace,H.batching=N.batching,H.batchingColor=N.batchingColor,H.instancing=N.instancing,H.instancingColor=N.instancingColor,H.instancingMorph=N.instancingMorph,H.skinning=N.skinning,H.morphTargets=N.morphTargets,H.morphNormals=N.morphNormals,H.morphColors=N.morphColors,H.morphTargetsCount=N.morphTargetsCount,H.numClippingPlanes=N.numClippingPlanes,H.numIntersection=N.numClipIntersection,H.vertexAlphas=N.vertexAlphas,H.vertexTangents=N.vertexTangents,H.toneMapping=N.toneMapping}function $_(_,N){if(_.length===0)return null;if(_.length===1)return _[0].texture!==null?_[0]:null;E.setFromMatrixPosition(N.matrixWorld);for(let H=0,U=_.length;H<U;H++){let B=_[H];if(B.texture!==null&&B.boundingBox.containsPoint(E))return B}return null}function q_(_,N,H,U,B){N.isScene!==!0&&(N=wt),g.resetTextureUnits();let de=N.fog,ve=U.isMeshStandardMaterial||U.isMeshLambertMaterial||U.isMeshPhongMaterial?N.environment:null,ue=P===null?L.outputColorSpace:P.isXRRenderTarget===!0?P.texture.colorSpace:ze.workingColorSpace,_e=U.isMeshStandardMaterial||U.isMeshLambertMaterial&&!U.envMap||U.isMeshPhongMaterial&&!U.envMap,Me=O.get(U.envMap||ve,_e),Pe=U.vertexColors===!0&&!!H.attributes.color&&H.attributes.color.itemSize===4,Ue=!!H.attributes.tangent&&(!!U.normalMap||U.anisotropy>0),Ee=!!H.morphAttributes.position,st=!!H.morphAttributes.normal,Mt=!!H.morphAttributes.color,vt=Fn;U.toneMapped&&(P===null||P.isXRRenderTarget===!0)&&(vt=L.toneMapping);let ct=H.morphAttributes.position||H.morphAttributes.normal||H.morphAttributes.color,Ft=ct!==void 0?ct.length:0,me=x.get(U),tn=b.state.lights;if(pt===!0&&(Ve===!0||_!==F)){let dt=_===F&&U.id===z;ge.setState(U,_,dt)}let Xe=!1;U.version===me.__version?(me.needsLights&&me.lightsStateVersion!==tn.state.version||me.outputColorSpace!==ue||B.isBatchedMesh&&me.batching===!1||!B.isBatchedMesh&&me.batching===!0||B.isBatchedMesh&&me.batchingColor===!0&&B.colorTexture===null||B.isBatchedMesh&&me.batchingColor===!1&&B.colorTexture!==null||B.isInstancedMesh&&me.instancing===!1||!B.isInstancedMesh&&me.instancing===!0||B.isSkinnedMesh&&me.skinning===!1||!B.isSkinnedMesh&&me.skinning===!0||B.isInstancedMesh&&me.instancingColor===!0&&B.instanceColor===null||B.isInstancedMesh&&me.instancingColor===!1&&B.instanceColor!==null||B.isInstancedMesh&&me.instancingMorph===!0&&B.morphTexture===null||B.isInstancedMesh&&me.instancingMorph===!1&&B.morphTexture!==null||me.envMap!==Me||U.fog===!0&&me.fog!==de||me.numClippingPlanes!==void 0&&(me.numClippingPlanes!==ge.numPlanes||me.numIntersection!==ge.numIntersection)||me.vertexAlphas!==Pe||me.vertexTangents!==Ue||me.morphTargets!==Ee||me.morphNormals!==st||me.morphColors!==Mt||me.toneMapping!==vt||me.morphTargetsCount!==Ft||!!me.lightProbeGrid!=b.state.lightProbeGridArray.length>0)&&(Xe=!0):(Xe=!0,me.__version=U.version);let mn=me.currentProgram;Xe===!0&&(mn=ca(U,N,B),k&&U.isNodeMaterial&&k.onUpdateProgram(U,mn,me));let Vn=!1,Mi=!1,Lr=!1,lt=mn.getUniforms(),St=me.uniforms;if(ae.useProgram(mn.program)&&(Vn=!0,Mi=!0,Lr=!0),U.id!==z&&(z=U.id,Mi=!0),me.needsLights){let dt=$_(b.state.lightProbeGridArray,B);me.lightProbeGrid!==dt&&(me.lightProbeGrid=dt,Mi=!0)}if(Vn||F!==_){ae.buffers.depth.getReversed()&&_.reversedDepth!==!0&&(_._reversedDepth=!0,_.updateProjectionMatrix()),lt.setValue(R,"projectionMatrix",_.projectionMatrix),lt.setValue(R,"viewMatrix",_.matrixWorldInverse);let Ei=lt.map.cameraPosition;Ei!==void 0&&Ei.setValue(R,ht.setFromMatrixPosition(_.matrixWorld)),ut.logarithmicDepthBuffer&&lt.setValue(R,"logDepthBufFC",2/(Math.log(_.far+1)/Math.LN2)),(U.isMeshPhongMaterial||U.isMeshToonMaterial||U.isMeshLambertMaterial||U.isMeshBasicMaterial||U.isMeshStandardMaterial||U.isShaderMaterial)&&lt.setValue(R,"isOrthographic",_.isOrthographicCamera===!0),F!==_&&(F=_,Mi=!0,Lr=!0)}if(me.needsLights&&(tn.state.directionalShadowMap.length>0&&lt.setValue(R,"directionalShadowMap",tn.state.directionalShadowMap,g),tn.state.spotShadowMap.length>0&&lt.setValue(R,"spotShadowMap",tn.state.spotShadowMap,g),tn.state.pointShadowMap.length>0&&lt.setValue(R,"pointShadowMap",tn.state.pointShadowMap,g)),B.isSkinnedMesh){lt.setOptional(R,B,"bindMatrix"),lt.setOptional(R,B,"bindMatrixInverse");let dt=B.skeleton;dt&&(dt.boneTexture===null&&dt.computeBoneTexture(),lt.setValue(R,"boneTexture",dt.boneTexture,g))}B.isBatchedMesh&&(lt.setOptional(R,B,"batchingTexture"),lt.setValue(R,"batchingTexture",B._matricesTexture,g),lt.setOptional(R,B,"batchingIdTexture"),lt.setValue(R,"batchingIdTexture",B._indirectTexture,g),lt.setOptional(R,B,"batchingColorTexture"),B._colorsTexture!==null&&lt.setValue(R,"batchingColorTexture",B._colorsTexture,g));let Si=H.morphAttributes;if((Si.position!==void 0||Si.normal!==void 0||Si.color!==void 0)&&Ce.update(B,H,mn),(Mi||me.receiveShadow!==B.receiveShadow)&&(me.receiveShadow=B.receiveShadow,lt.setValue(R,"receiveShadow",B.receiveShadow)),(U.isMeshStandardMaterial||U.isMeshLambertMaterial||U.isMeshPhongMaterial)&&U.envMap===null&&N.environment!==null&&(St.envMapIntensity.value=N.environmentIntensity),St.dfgLUT!==void 0&&(St.dfgLUT.value=xA()),Mi){if(lt.setValue(R,"toneMappingExposure",L.toneMappingExposure),me.needsLights&&X_(St,Lr),de&&U.fog===!0&&G.refreshFogUniforms(St,de),G.refreshMaterialUniforms(St,U,Oe,nt,b.state.transmissionRenderTarget[_.id]),me.needsLights&&me.lightProbeGrid){let dt=me.lightProbeGrid;St.probesSH.value=dt.texture,St.probesMin.value.copy(dt.boundingBox.min),St.probesMax.value.copy(dt.boundingBox.max),St.probesResolution.value.copy(dt.resolution)}ks.upload(R,Np(me),St,g)}if(U.isShaderMaterial&&U.uniformsNeedUpdate===!0&&(ks.upload(R,Np(me),St,g),U.uniformsNeedUpdate=!1),U.isSpriteMaterial&&lt.setValue(R,"center",B.center),lt.setValue(R,"modelViewMatrix",B.modelViewMatrix),lt.setValue(R,"normalMatrix",B.normalMatrix),lt.setValue(R,"modelMatrix",B.matrixWorld),U.uniformsGroups!==void 0){let dt=U.uniformsGroups;for(let Ei=0,Or=dt.length;Ei<Or;Ei++){let Lp=dt[Ei];j.update(Lp,mn),j.bind(Lp,mn)}}return mn}function X_(_,N){_.ambientLightColor.needsUpdate=N,_.lightProbe.needsUpdate=N,_.directionalLights.needsUpdate=N,_.directionalLightShadows.needsUpdate=N,_.pointLights.needsUpdate=N,_.pointLightShadows.needsUpdate=N,_.spotLights.needsUpdate=N,_.spotLightShadows.needsUpdate=N,_.rectAreaLights.needsUpdate=N,_.hemisphereLights.needsUpdate=N}function Y_(_){return _.isMeshLambertMaterial||_.isMeshToonMaterial||_.isMeshPhongMaterial||_.isMeshStandardMaterial||_.isShadowMaterial||_.isShaderMaterial&&_.lights===!0}this.getActiveCubeFace=function(){return W},this.getActiveMipmapLevel=function(){return X},this.getRenderTarget=function(){return P},this.setRenderTargetTextures=function(_,N,H){let U=x.get(_);U.__autoAllocateDepthBuffer=_.resolveDepthBuffer===!1,U.__autoAllocateDepthBuffer===!1&&(U.__useRenderToTexture=!1),x.get(_.texture).__webglTexture=N,x.get(_.depthTexture).__webglTexture=U.__autoAllocateDepthBuffer?void 0:H,U.__hasExternalTextures=!0},this.setRenderTargetFramebuffer=function(_,N){let H=x.get(_);H.__webglFramebuffer=N,H.__useDefaultFramebuffer=N===void 0};let Z_=R.createFramebuffer();this.setRenderTarget=function(_,N=0,H=0){P=_,W=N,X=H;let U=null,B=!1,de=!1;if(_){let ue=x.get(_);if(ue.__useDefaultFramebuffer!==void 0){ae.bindFramebuffer(R.FRAMEBUFFER,ue.__webglFramebuffer),K.copy(_.viewport),Q.copy(_.scissor),le=_.scissorTest,ae.viewport(K),ae.scissor(Q),ae.setScissorTest(le),z=-1;return}else if(ue.__webglFramebuffer===void 0)g.setupRenderTarget(_);else if(ue.__hasExternalTextures)g.rebindTextures(_,x.get(_.texture).__webglTexture,x.get(_.depthTexture).__webglTexture);else if(_.depthBuffer){let Pe=_.depthTexture;if(ue.__boundDepthTexture!==Pe){if(Pe!==null&&x.has(Pe)&&(_.width!==Pe.image.width||_.height!==Pe.image.height))throw new Error("WebGLRenderTarget: Attached DepthTexture is initialized to the incorrect size.");g.setupDepthRenderbuffer(_)}}let _e=_.texture;(_e.isData3DTexture||_e.isDataArrayTexture||_e.isCompressedArrayTexture)&&(de=!0);let Me=x.get(_).__webglFramebuffer;_.isWebGLCubeRenderTarget?(Array.isArray(Me[N])?U=Me[N][H]:U=Me[N],B=!0):_.samples>0&&g.useMultisampledRTT(_)===!1?U=x.get(_).__webglMultisampledFramebuffer:Array.isArray(Me)?U=Me[H]:U=Me,K.copy(_.viewport),Q.copy(_.scissor),le=_.scissorTest}else K.copy(ie).multiplyScalar(Oe).floor(),Q.copy(Te).multiplyScalar(Oe).floor(),le=De;if(H!==0&&(U=Z_),ae.bindFramebuffer(R.FRAMEBUFFER,U)&&ae.drawBuffers(_,U),ae.viewport(K),ae.scissor(Q),ae.setScissorTest(le),B){let ue=x.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_CUBE_MAP_POSITIVE_X+N,ue.__webglTexture,H)}else if(de){let ue=N;for(let _e=0;_e<_.textures.length;_e++){let Me=x.get(_.textures[_e]);R.framebufferTextureLayer(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0+_e,Me.__webglTexture,H,ue)}}else if(_!==null&&H!==0){let ue=x.get(_.texture);R.framebufferTexture2D(R.FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,ue.__webglTexture,H)}z=-1},this.readRenderTargetPixels=function(_,N,H,U,B,de,ve,ue=0){if(!(_&&_.isWebGLRenderTarget)){we("WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");return}let _e=x.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ve!==void 0&&(_e=_e[ve]),_e){ae.bindFramebuffer(R.FRAMEBUFFER,_e);try{let Me=_.textures[ue],Pe=Me.format,Ue=Me.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+ue),!ut.textureFormatReadable(Pe)){we("WebGLRenderer.readRenderTargetPixels: renderTarget is not in RGBA or implementation defined format.");return}if(!ut.textureTypeReadable(Ue)){we("WebGLRenderer.readRenderTargetPixels: renderTarget is not in UnsignedByteType or implementation defined type.");return}N>=0&&N<=_.width-U&&H>=0&&H<=_.height-B&&R.readPixels(N,H,U,B,C.convert(Pe),C.convert(Ue),de)}finally{let Me=P!==null?x.get(P).__webglFramebuffer:null;ae.bindFramebuffer(R.FRAMEBUFFER,Me)}}},this.readRenderTargetPixelsAsync=async function(_,N,H,U,B,de,ve,ue=0){if(!(_&&_.isWebGLRenderTarget))throw new Error("THREE.WebGLRenderer.readRenderTargetPixels: renderTarget is not THREE.WebGLRenderTarget.");let _e=x.get(_).__webglFramebuffer;if(_.isWebGLCubeRenderTarget&&ve!==void 0&&(_e=_e[ve]),_e)if(N>=0&&N<=_.width-U&&H>=0&&H<=_.height-B){ae.bindFramebuffer(R.FRAMEBUFFER,_e);let Me=_.textures[ue],Pe=Me.format,Ue=Me.type;if(_.textures.length>1&&R.readBuffer(R.COLOR_ATTACHMENT0+ue),!ut.textureFormatReadable(Pe))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in RGBA or implementation defined format.");if(!ut.textureTypeReadable(Ue))throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: renderTarget is not in UnsignedByteType or implementation defined type.");let Ee=R.createBuffer();R.bindBuffer(R.PIXEL_PACK_BUFFER,Ee),R.bufferData(R.PIXEL_PACK_BUFFER,de.byteLength,R.STREAM_READ),R.readPixels(N,H,U,B,C.convert(Pe),C.convert(Ue),0);let st=P!==null?x.get(P).__webglFramebuffer:null;ae.bindFramebuffer(R.FRAMEBUFFER,st);let Mt=R.fenceSync(R.SYNC_GPU_COMMANDS_COMPLETE,0);return R.flush(),await d_(R,Mt,4),R.bindBuffer(R.PIXEL_PACK_BUFFER,Ee),R.getBufferSubData(R.PIXEL_PACK_BUFFER,0,de),R.deleteBuffer(Ee),R.deleteSync(Mt),de}else throw new Error("THREE.WebGLRenderer.readRenderTargetPixelsAsync: requested read bounds are out of range.")},this.copyFramebufferToTexture=function(_,N=null,H=0){let U=Math.pow(2,-H),B=Math.floor(_.image.width*U),de=Math.floor(_.image.height*U),ve=N!==null?N.x:0,ue=N!==null?N.y:0;g.setTexture2D(_,0),R.copyTexSubImage2D(R.TEXTURE_2D,H,0,0,ve,ue,B,de),ae.unbindTexture()};let J_=R.createFramebuffer(),K_=R.createFramebuffer();this.copyTextureToTexture=function(_,N,H=null,U=null,B=0,de=0){let ve,ue,_e,Me,Pe,Ue,Ee,st,Mt,vt=_.isCompressedTexture?_.mipmaps[de]:_.image;if(H!==null)ve=H.max.x-H.min.x,ue=H.max.y-H.min.y,_e=H.isBox3?H.max.z-H.min.z:1,Me=H.min.x,Pe=H.min.y,Ue=H.isBox3?H.min.z:0;else{let St=Math.pow(2,-B);ve=Math.floor(vt.width*St),ue=Math.floor(vt.height*St),_.isDataArrayTexture?_e=vt.depth:_.isData3DTexture?_e=Math.floor(vt.depth*St):_e=1,Me=0,Pe=0,Ue=0}U!==null?(Ee=U.x,st=U.y,Mt=U.z):(Ee=0,st=0,Mt=0);let ct=C.convert(N.format),Ft=C.convert(N.type),me;N.isData3DTexture?(g.setTexture3D(N,0),me=R.TEXTURE_3D):N.isDataArrayTexture||N.isCompressedArrayTexture?(g.setTexture2DArray(N,0),me=R.TEXTURE_2D_ARRAY):(g.setTexture2D(N,0),me=R.TEXTURE_2D),ae.activeTexture(R.TEXTURE0),ae.pixelStorei(R.UNPACK_FLIP_Y_WEBGL,N.flipY),ae.pixelStorei(R.UNPACK_PREMULTIPLY_ALPHA_WEBGL,N.premultiplyAlpha),ae.pixelStorei(R.UNPACK_ALIGNMENT,N.unpackAlignment);let tn=ae.getParameter(R.UNPACK_ROW_LENGTH),Xe=ae.getParameter(R.UNPACK_IMAGE_HEIGHT),mn=ae.getParameter(R.UNPACK_SKIP_PIXELS),Vn=ae.getParameter(R.UNPACK_SKIP_ROWS),Mi=ae.getParameter(R.UNPACK_SKIP_IMAGES);ae.pixelStorei(R.UNPACK_ROW_LENGTH,vt.width),ae.pixelStorei(R.UNPACK_IMAGE_HEIGHT,vt.height),ae.pixelStorei(R.UNPACK_SKIP_PIXELS,Me),ae.pixelStorei(R.UNPACK_SKIP_ROWS,Pe),ae.pixelStorei(R.UNPACK_SKIP_IMAGES,Ue);let Lr=_.isDataArrayTexture||_.isData3DTexture,lt=N.isDataArrayTexture||N.isData3DTexture;if(_.isDepthTexture){let St=x.get(_),Si=x.get(N),dt=x.get(St.__renderTarget),Ei=x.get(Si.__renderTarget);ae.bindFramebuffer(R.READ_FRAMEBUFFER,dt.__webglFramebuffer),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,Ei.__webglFramebuffer);for(let Or=0;Or<_e;Or++)Lr&&(R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,x.get(_).__webglTexture,B,Ue+Or),R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,x.get(N).__webglTexture,de,Mt+Or)),R.blitFramebuffer(Me,Pe,ve,ue,Ee,st,ve,ue,R.DEPTH_BUFFER_BIT,R.NEAREST);ae.bindFramebuffer(R.READ_FRAMEBUFFER,null),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else if(B!==0||_.isRenderTargetTexture||x.has(_)){let St=x.get(_),Si=x.get(N);ae.bindFramebuffer(R.READ_FRAMEBUFFER,J_),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,K_);for(let dt=0;dt<_e;dt++)Lr?R.framebufferTextureLayer(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,St.__webglTexture,B,Ue+dt):R.framebufferTexture2D(R.READ_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,St.__webglTexture,B),lt?R.framebufferTextureLayer(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,Si.__webglTexture,de,Mt+dt):R.framebufferTexture2D(R.DRAW_FRAMEBUFFER,R.COLOR_ATTACHMENT0,R.TEXTURE_2D,Si.__webglTexture,de),B!==0?R.blitFramebuffer(Me,Pe,ve,ue,Ee,st,ve,ue,R.COLOR_BUFFER_BIT,R.NEAREST):lt?R.copyTexSubImage3D(me,de,Ee,st,Mt+dt,Me,Pe,ve,ue):R.copyTexSubImage2D(me,de,Ee,st,Me,Pe,ve,ue);ae.bindFramebuffer(R.READ_FRAMEBUFFER,null),ae.bindFramebuffer(R.DRAW_FRAMEBUFFER,null)}else lt?_.isDataTexture||_.isData3DTexture?R.texSubImage3D(me,de,Ee,st,Mt,ve,ue,_e,ct,Ft,vt.data):N.isCompressedArrayTexture?R.compressedTexSubImage3D(me,de,Ee,st,Mt,ve,ue,_e,ct,vt.data):R.texSubImage3D(me,de,Ee,st,Mt,ve,ue,_e,ct,Ft,vt):_.isDataTexture?R.texSubImage2D(R.TEXTURE_2D,de,Ee,st,ve,ue,ct,Ft,vt.data):_.isCompressedTexture?R.compressedTexSubImage2D(R.TEXTURE_2D,de,Ee,st,vt.width,vt.height,ct,vt.data):R.texSubImage2D(R.TEXTURE_2D,de,Ee,st,ve,ue,ct,Ft,vt);ae.pixelStorei(R.UNPACK_ROW_LENGTH,tn),ae.pixelStorei(R.UNPACK_IMAGE_HEIGHT,Xe),ae.pixelStorei(R.UNPACK_SKIP_PIXELS,mn),ae.pixelStorei(R.UNPACK_SKIP_ROWS,Vn),ae.pixelStorei(R.UNPACK_SKIP_IMAGES,Mi),de===0&&N.generateMipmaps&&R.generateMipmap(me),ae.unbindTexture()},this.initRenderTarget=function(_){x.get(_).__webglFramebuffer===void 0&&g.setupRenderTarget(_)},this.initTexture=function(_){_.isCubeTexture?g.setTextureCube(_,0):_.isData3DTexture?g.setTexture3D(_,0):_.isDataArrayTexture||_.isCompressedArrayTexture?g.setTexture2DArray(_,0):g.setTexture2D(_,0),ae.unbindTexture()},this.resetState=function(){W=0,X=0,P=null,ae.reset(),ne.reset()},typeof __THREE_DEVTOOLS__<"u"&&__THREE_DEVTOOLS__.dispatchEvent(new CustomEvent("observe",{detail:this}))}get coordinateSystem(){return On}get outputColorSpace(){return this._outputColorSpace}set outputColorSpace(e){this._outputColorSpace=e;let t=this.getContext();t.drawingBufferColorSpace=ze._getDrawingBufferColorSpace(e),t.unpackColorSpace=ze._getUnpackColorSpace()}};var SA=["canvasRef"],yu=class n{canvasRef;scene=null;camera=null;renderer=null;mesh=null;uniforms=null;animationId=null;ngZone=$e(Zt);platformId=$e(gr);ngAfterViewInit(){Qf(this.platformId)&&this.ngZone.runOutsideAngular(()=>{this.initScene(),this.animate(),window.addEventListener("resize",this.handleResize)})}ngOnDestroy(){Qf(this.platformId)&&(this.animationId&&cancelAnimationFrame(this.animationId),window.removeEventListener("resize",this.handleResize),this.mesh&&(this.scene?.remove(this.mesh),this.mesh.geometry.dispose(),this.mesh.material instanceof vi&&this.mesh.material.dispose()),this.renderer?.dispose())}initScene(){let e=this.canvasRef.nativeElement,t=`
      attribute vec3 position;
      void main() {
        gl_Position = vec4(position, 1.0);
      }
    `,i=`
      precision highp float;
      uniform vec2 resolution;
      uniform float time;

      float getWaveHeight(float x, float t, float speed) {
        float w1 = sin(x * 1.0 + t * 0.7 * speed) * 0.22;
        float w2 = sin(x * 2.1 - t * 1.1 * speed) * 0.08;
        float w3 = cos(x * 0.55 + t * 0.35 * speed) * 0.12;
        float w4 = sin(x * 3.2 + t * 1.5 * speed) * 0.03;
        return w1 + w2 + w3 + w4;
      }

      void main() {
        float aspect = resolution.x / resolution.y;
        
        // Map coordinates: center is (0,0), y ranges from -1.0 to 1.0, x from -aspect to aspect
        vec2 p = (gl_FragCoord.xy * 2.0 - resolution) / resolution.y;

        // Dark background with subtle vertical gradient for depth
        vec3 bg = vec3(0.003, 0.003, 0.007) * (1.0 - p.y * 0.4);
        
        // Premium curated color palette
        vec3 colorR = vec3(1.0, 0.25, 0.55);   // Vibrant Pink/Magenta
        vec3 colorG = vec3(0.12, 0.85, 0.75);  // Vibrant Mint/Cyan
        vec3 colorB = vec3(0.15, 0.45, 1.0);   // Electric Royal Blue

        // Constant physical screen-space chromatic aberration (e.g. 14 physical pixels)
        float aberration = 14.0 / resolution.y;

        // Evaluate wave positions for each channel with slightly different speed multipliers
        float waveR = p.y + getWaveHeight(p.x - aberration, time, 1.0);
        float waveG = p.y + getWaveHeight(p.x, time, 1.08);
        float waveB = p.y + getWaveHeight(p.x + aberration, time, 0.92);

        // Core intensity (sharp center) and halo intensity (wide outer glow)
        // Adding epsilon avoids division by zero and provides smooth anti-aliasing
        float coreR = 0.004 / (abs(waveR) + 0.012);
        float coreG = 0.004 / (abs(waveG) + 0.012);
        float coreB = 0.004 / (abs(waveB) + 0.012);

        float glowR = 0.012 / (abs(waveR) + 0.06);
        float glowG = 0.012 / (abs(waveG) + 0.06);
        float glowB = 0.012 / (abs(waveB) + 0.06);

        // Blend the cores and glows for each component
        vec3 rgb = colorR * (coreR + glowR * 0.5) +
                   colorG * (coreG + glowG * 0.5) +
                   colorB * (coreB + glowB * 0.5);

        // Fade out glow smoothly near the horizontal screen edges
        float edgeFade = smoothstep(aspect, aspect - 0.25, abs(p.x));
        rgb *= edgeFade;

        gl_FragColor = vec4(bg + rgb, 1.0);
      }
    `;this.scene=new Uo,this.renderer=new mu({canvas:e}),this.renderer.setPixelRatio(window.devicePixelRatio),this.renderer.setClearColor(new Ze(0)),this.camera=new Ar(-1,1,1,-1,0,-1);let r=window.devicePixelRatio||1;this.uniforms={resolution:{value:[window.innerWidth*r,window.innerHeight*r]},time:{value:0}};let s=[-1,-1,0,1,-1,0,-1,1,0,1,-1,0,-1,1,0,1,1,0],o=new $t(new Float32Array(s),3),a=new Sn;a.setAttribute("position",o);let c=new Ns({vertexShader:t,fragmentShader:i,uniforms:this.uniforms,side:En});this.mesh=new Qt(a,c),this.scene.add(this.mesh),this.handleResize()}animate=()=>{this.uniforms&&(this.uniforms.time.value+=.008),this.renderer&&this.scene&&this.camera&&this.renderer.render(this.scene,this.camera),this.animationId=requestAnimationFrame(this.animate)};handleResize=()=>{if(!this.renderer||!this.uniforms)return;let e=window.innerWidth,t=window.innerHeight,i=window.devicePixelRatio||1;this.renderer.setSize(e,t,!1),this.uniforms.resolution.value=[e*i,t*i]};static \u0275fac=function(t){return new(t||n)};static \u0275cmp=yr({type:n,selectors:[["app-web-gl-shader"]],viewQuery:function(t,i){if(t&1&&yc(SA,5),t&2){let r;Gf(r=Wf())&&(i.canvasRef=r.first)}},decls:2,vars:0,consts:[["canvasRef",""],[1,"fixed","top-0","left-0","w-full","h-full","block","z-0","pointer-events-none"]],template:function(t,i){t&1&&xr(0,"canvas",1,0)},encapsulation:2,changeDetection:0})};var EA=["*"],_u=class n{variant=Yf("default");size=Yf("xxl");baseClasses="relative inline-flex items-center transition-colors justify-center cursor-pointer gap-2 whitespace-nowrap text-sm font-medium transition-[color,box-shadow] disabled:pointer-events-none disabled:opacity-50 outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive text-white border border-white/20 rounded-full";getClasses(){let e=this.baseClasses;switch(this.variant()){case"default":e+=" bg-transparent hover:scale-105 duration-300 transition text-white";break;case"outline":e+=" border border-input bg-background hover:bg-accent hover:text-accent-foreground";break;case"destructive":e+=" bg-destructive text-white hover:bg-destructive/90 focus-visible:ring-destructive/20 dark:focus-visible:ring-destructive/40";break}switch(this.size()){case"sm":e+=" h-8 text-xs gap-1.5 px-4";break;case"default":e+=" h-9 px-4 py-2";break;case"lg":e+=" h-10 px-6";break;case"xl":e+=" h-12 px-8";break;case"xxl":e+=" h-14 px-10";break}return e}static \u0275fac=function(t){return new(t||n)};static \u0275cmp=yr({type:n,selectors:[["app-liquid-button"]],hostAttrs:[1,"inline-flex"],inputs:{variant:[1,"variant"],size:[1,"size"]},ngContentSelectors:EA,decls:13,vars:2,consts:[[1,"absolute","top-0","left-0","z-0","h-full","w-full","rounded-full","shadow-[0_0_6px_rgba(0,0,0,0.03),0_2px_6px_rgba(0,0,0,0.08),inset_3px_3px_0.5px_-3px_rgba(0,0,0,0.9),inset_-3px_-3px_0.5px_-3px_rgba(0,0,0,0.85),inset_1px_1px_1px_-0.5px_rgba(0,0,0,0.6),inset_-1px_-1px_1px_-0.5px_rgba(0,0,0,0.6),inset_0_0_6px_6px_rgba(0,0,0,0.12),inset_0_0_2px_2px_rgba(0,0,0,0.06),0_0_12px_rgba(255,255,255,0.15)]","transition-all","dark:shadow-[0_0_8px_rgba(0,0,0,0.03),0_2px_6px_rgba(0,0,0,0.08),inset_3px_3px_0.5px_-3.5px_rgba(255,255,255,0.09),inset_-3px_-3px_0.5px_-3.5px_rgba(255,255,255,0.85),inset_1px_1px_1px_-0.5px_rgba(255,255,255,0.6),inset_-1px_-1px_1px_-0.5px_rgba(255,255,255,0.6),inset_0_0_6px_6px_rgba(255,255,255,0.12),inset_0_0_2px_2px_rgba(255,255,255,0.06),0_0_12px_rgba(0,0,0,0.15)]"],[1,"absolute","top-0","left-0","isolate","-z-10","h-full","w-full","overflow-hidden","rounded-full",2,"backdrop-filter","url('#container-glass')"],[1,"pointer-events-none","z-10","relative"],["width","0","height","0",1,"hidden"],["id","container-glass","x","0%","y","0%","width","100%","height","100%","color-interpolation-filters","sRGB"],["type","fractalNoise","baseFrequency","0.05 0.05","numOctaves","1","seed","1","result","turbulence"],["in","turbulence","stdDeviation","2","result","blurredNoise"],["in","SourceGraphic","in2","blurredNoise","scale","70","xChannelSelector","R","yChannelSelector","B","result","displaced"],["in","displaced","stdDeviation","4","result","finalBlur"],["in","finalBlur","in2","finalBlur","operator","over"]],template:function(t,i){t&1&&(Hf(),_r(0,"button"),xr(1,"div",0)(2,"div",1),_r(3,"div",2),zf(4),fs(),fr(),_r(5,"svg",3)(6,"defs")(7,"filter",4),xr(8,"feTurbulence",5)(9,"feGaussianBlur",6)(10,"feDisplacementMap",7)(11,"feGaussianBlur",8)(12,"feComposite",9),fs()()()()),t&2&&jf(i.getClasses())},encapsulation:2,changeDetection:0})};var xu=class n{static \u0275fac=function(t){return new(t||n)};static \u0275cmp=yr({type:n,selectors:[["app-root"]],decls:20,vars:0,consts:[[1,"relative","flex","min-h-screen","w-full","flex-col","items-center","justify-center","overflow-hidden","text-white","dark","bg-transparent"],[1,"relative","z-10","w-full","max-w-[1024px]","mx-auto","px-4","flex","flex-col","items-center","justify-center","pointer-events-none","mb-24","md:mb-40"],[1,"text-white","text-center","text-4xl","sm:text-5xl","md:text-6xl","lg:text-7xl","font-extralight","tracking-tight","leading-tight","mb-6"],[1,"text-white/70","text-center","text-lg","md:text-xl","font-extralight","mb-10","max-w-lg","mx-auto"],[1,"flex","items-center","justify-center","gap-4","sm:gap-6","pointer-events-auto","flex-wrap"],["href","https://www.linkedin.com/in/hyagorios","target","_blank","rel","noopener noreferrer",1,"group","outline-none"],["variant","default","size","lg"],[1,"flex","items-center","gap-2"],["xmlns","http://www.w3.org/2000/svg","width","20","height","20","viewBox","0 0 24 24","fill","currentColor",1,"group-hover:scale-110","transition-transform"],["d","M20.447 20.452h-3.554v-5.569c0-1.328-.027-3.037-1.852-3.037-1.853 0-2.136 1.445-2.136 2.939v5.667H9.351V9h3.414v1.561h.046c.477-.9 1.637-1.85 3.37-1.85 3.601 0 4.267 2.37 4.267 5.455v6.286zM5.337 7.433c-1.144 0-2.063-.926-2.063-2.065 0-1.138.92-2.063 2.063-2.063 1.14 0 2.064.925 2.064 2.063 0 1.139-.925 2.065-2.064 2.065zm1.782 13.019H3.555V9h3.564v11.452zM22.225 0H1.771C.792 0 0 .774 0 1.729v20.542C0 23.227.792 24 1.771 24h20.451C23.2 24 24 23.227 24 22.271V1.729C24 .774 23.2 0 22.222 0h.003z"],["href","https://github.com/hyagoriosdev","target","_blank","rel","noopener noreferrer",1,"group","outline-none"],["xmlns","http://www.w3.org/2000/svg","width","20","height","20","viewBox","0 0 24 24","fill","currentColor","stroke","none",1,"group-hover:scale-110","transition-transform"],["d","M12 0c-6.626 0-12 5.373-12 12 0 5.302 3.438 9.8 8.207 11.387.599.111.793-.261.793-.577v-2.234c-3.338.726-4.033-1.416-4.033-1.416-.546-1.387-1.333-1.756-1.333-1.756-1.089-.745.083-.729.083-.729 1.205.084 1.839 1.237 1.839 1.237 1.07 1.834 2.807 1.304 3.492.997.107-.775.418-1.305.762-1.604-2.665-.305-5.467-1.334-5.467-5.931 0-1.311.469-2.381 1.236-3.221-.124-.303-.535-1.524.117-3.176 0 0 1.008-.322 3.301 1.23.957-.266 1.983-.399 3.003-.404 1.02.005 2.047.138 3.006.404 2.291-1.552 3.297-1.23 3.297-1.23.653 1.653.242 2.874.118 3.176.77.84 1.235 1.911 1.235 3.221 0 4.609-2.807 5.624-5.479 5.921.43.372.823 1.102.823 2.222v3.293c0 .319.192.694.801.576 4.765-1.589 8.199-6.086 8.199-11.386 0-6.627-5.373-12-12-12z"]],template:function(t,i){t&1&&(_n(0,"main",0),ds(1,"app-web-gl-shader"),_n(2,"div",1)(3,"h1",2),hs(4," Under Improvements "),$n(),_n(5,"p",3),hs(6," In the meantime, please take a look at "),$n(),_n(7,"div",4)(8,"a",5)(9,"app-liquid-button",6)(10,"span",7),fr(),_n(11,"svg",8),ds(12,"path",9),$n(),hs(13," LinkedIn "),$n()()(),Ga(),_n(14,"a",10)(15,"app-liquid-button",6)(16,"span",7),fr(),_n(17,"svg",11),ds(18,"path",12),$n(),hs(19," GitHub "),$n()()()()()())},dependencies:[yu,_u],encapsulation:2,changeDetection:0})};var W_={providers:[Hd()]};ah(xu,W_).catch(n=>console.error(n));
