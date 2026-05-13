import { useEffect, useRef, useState } from 'react';

function LoadingOverlay({ workspace }) {
  return (
    <div style={{ position:'fixed', inset:0, zIndex:9999, background:'#0A1020', display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', gap:'28px' }}>
      <style>{`@keyframes lp-spin{to{transform:rotate(360deg)}}@keyframes lp-spinR{to{transform:rotate(-360deg)}}@keyframes lp-pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.8;transform:scale(1.04)}}`}</style>
      <div style={{ position:'relative', width:'130px', height:'130px' }}>
        <div style={{ position:'absolute', inset:0, border:'1.5px solid rgba(201,168,76,0.2)', borderTopColor:'#C9A84C', borderRadius:'50%', animation:'lp-spin 1.4s linear infinite' }} />
        <div style={{ position:'absolute', inset:'14px', border:'1px solid rgba(201,168,76,0.1)', borderBottomColor:'rgba(201,168,76,0.5)', borderRadius:'50%', animation:'lp-spinR 2s linear infinite' }} />
        <div style={{ position:'absolute', inset:0, display:'flex', alignItems:'center', justifyContent:'center' }}>
          <img src="/logo.png" alt="NBE" style={{ width:'64px', height:'64px', objectFit:'contain', animation:'lp-pulse 2s ease-in-out infinite' }} />
        </div>
      </div>
      <div style={{ textAlign:'center' }}>
        <div style={{ fontSize:'11px', color:'#C9A84C', fontWeight:'700', letterSpacing:'0.14em', textTransform:'uppercase', marginBottom:'6px' }}>{workspace === 'aml' ? 'Activating AML Workspace' : 'Loading Workspace'}</div>
        <div style={{ fontSize:'12px', color:'#4B5563' }}>National Bank of Ethiopia</div>
      </div>
    </div>
  );
}

function BgCanvas() {
  const ref = useRef(null);
  useEffect(() => {
    const c = ref.current; if (!c) return;
    const ctx = c.getContext('2d'); let id;
    const resize = () => { c.width = c.offsetWidth; c.height = c.offsetHeight; };
    resize(); window.addEventListener('resize', resize);
    const nodes = Array.from({ length: 44 }, () => ({ x:Math.random()*c.width, y:Math.random()*c.height, vx:(Math.random()-.5)*.15, vy:(Math.random()-.5)*.15, r:Math.random()*1.8+.7 }));
    const draw = () => {
      ctx.clearRect(0,0,c.width,c.height);
      nodes.forEach(n => { n.x+=n.vx; n.y+=n.vy; if(n.x<0||n.x>c.width)n.vx*=-1; if(n.y<0||n.y>c.height)n.vy*=-1; });
      for(let i=0;i<nodes.length;i++) for(let j=i+1;j<nodes.length;j++) { const dx=nodes[i].x-nodes[j].x,dy=nodes[i].y-nodes[j].y,d=Math.sqrt(dx*dx+dy*dy); if(d<130){ctx.beginPath();ctx.moveTo(nodes[i].x,nodes[i].y);ctx.lineTo(nodes[j].x,nodes[j].y);ctx.strokeStyle=`rgba(201,168,76,${.065*(1-d/130)})`;ctx.lineWidth=.5;ctx.stroke();} }
      nodes.forEach(n=>{ctx.beginPath();ctx.arc(n.x,n.y,n.r,0,Math.PI*2);ctx.fillStyle='rgba(201,168,76,0.22)';ctx.fill();});
      id=requestAnimationFrame(draw);
    };
    draw();
    return () => { cancelAnimationFrame(id); window.removeEventListener('resize', resize); };
  }, []);
  return <canvas ref={ref} style={{ position:'absolute', inset:0, width:'100%', height:'100%', pointerEvents:'none', opacity:.6 }} />;
}

function AMLIcon() {
  return (
    <svg width="56" height="56" viewBox="0 0 56 56" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="28" cy="28" r="12" stroke="#C9A84C" strokeWidth="1.5" fill="none" opacity="0.9"/>
      <circle cx="28" cy="28" r="4" fill="#C9A84C" opacity="0.85"/>
      <circle cx="10" cy="18" r="3" fill="#C9A84C" opacity="0.6"/>
      <circle cx="46" cy="18" r="3" fill="#C9A84C" opacity="0.6"/>
      <circle cx="10" cy="38" r="3" fill="#C9A84C" opacity="0.6"/>
      <circle cx="46" cy="38" r="3" fill="#C9A84C" opacity="0.6"/>
      <circle cx="28" cy="6" r="3" fill="#C9A84C" opacity="0.5"/>
      <circle cx="28" cy="50" r="3" fill="#C9A84C" opacity="0.5"/>
      <line x1="28" y1="16" x2="10" y2="18" stroke="#C9A84C" strokeWidth="1" opacity="0.4"/>
      <line x1="28" y1="16" x2="46" y2="18" stroke="#C9A84C" strokeWidth="1" opacity="0.4"/>
      <line x1="28" y1="40" x2="10" y2="38" stroke="#C9A84C" strokeWidth="1" opacity="0.4"/>
      <line x1="28" y1="40" x2="46" y2="38" stroke="#C9A84C" strokeWidth="1" opacity="0.4"/>
      <line x1="28" y1="16" x2="28" y2="6" stroke="#C9A84C" strokeWidth="1" opacity="0.35"/>
      <line x1="28" y1="40" x2="28" y2="50" stroke="#C9A84C" strokeWidth="1" opacity="0.35"/>
      <path d="M20 28 L28 20 L36 28 L28 36 Z" stroke="#C9A84C" strokeWidth="1" fill="rgba(201,168,76,0.08)" opacity="0.7"/>
    </svg>
  );
}

function Card({ amlIcon, icon, title, subtitle, features, disabled, buttonLabel, onLaunch, delay }) {
  const [h, setH] = useState(false);
  return (
    <div onMouseEnter={()=>!disabled&&setH(true)} onMouseLeave={()=>setH(false)} style={{ flex:'1 1 280px', maxWidth:'360px', background:h?'linear-gradient(145deg,rgba(201,168,76,0.08),rgba(12,18,32,.97))':'rgba(11,17,30,.72)', border:`1px solid ${h&&!disabled?'rgba(201,168,76,.42)':disabled?'rgba(71,85,105,.2)':'rgba(201,168,76,.16)'}`, borderRadius:'16px', padding:'24px 22px', backdropFilter:'blur(16px)', transition:'all .3s ease', transform:h&&!disabled?'translateY(-4px) scale(1.01)':'translateY(0) scale(1)', boxShadow:h&&!disabled?'0 12px 40px rgba(201,168,76,0.1)':'0 4px 20px rgba(0,0,0,.3)', opacity:disabled?.48:1, cursor:disabled?'default':'pointer', animation:`lp-cardIn .55s ease ${delay}ms both` }}>
      <div style={{ width:'52px', height:'52px', borderRadius:'12px', background:disabled?'rgba(71,85,105,.08)':'rgba(201,168,76,0.06)', border:`1px solid ${disabled?'rgba(71,85,105,.15)':'rgba(201,168,76,.15)'}`, display:'flex', alignItems:'center', justifyContent:'center', marginBottom:'16px', overflow:'hidden' }}>
        {amlIcon ? <AMLIcon /> : <span style={{ fontSize:'22px' }}>{icon}</span>}
      </div>
      <div style={{ fontSize:'15px', fontWeight:'700', color:disabled?'#4B5563':'#C8B98A', marginBottom:'6px' }}>{title}</div>
      <div style={{ fontSize:'12px', color:'#3D4F63', lineHeight:1.65, marginBottom:'14px' }}>{subtitle}</div>
      <div style={{ display:'flex', flexDirection:'column', gap:'5px', marginBottom:'18px' }}>
        {features.map(f=>(
          <div key={f} style={{ display:'flex', alignItems:'center', gap:'8px' }}>
            <div style={{ width:'3px', height:'3px', borderRadius:'50%', background:disabled?'#2D3748':'rgba(201,168,76,.45)', flexShrink:0 }} />
            <span style={{ fontSize:'11px', color:disabled?'#2D3748':'#556070' }}>{f}</span>
          </div>
        ))}
      </div>
      <button onClick={disabled?undefined:onLaunch} disabled={disabled} style={{ width:'100%', padding:'10px', borderRadius:'8px', border:`1px solid ${disabled?'rgba(71,85,105,.15)':h?'rgba(201,168,76,.5)':'rgba(201,168,76,.22)'}`, background:disabled?'transparent':h?'rgba(201,168,76,.14)':'rgba(201,168,76,.06)', color:disabled?'#2D3748':h?'#E2C97E':'#B8963E', fontSize:'12px', fontWeight:'700', cursor:disabled?'not-allowed':'pointer', transition:'all .2s ease', letterSpacing:'.05em' }}>{buttonLabel}</button>
    </div>
  );
}

export default function LandingPage({ onEnterAML, onEnterCluster }) {
  const [launching, setLaunching] = useState(null);
  const go = (type, cb) => { setLaunching(type); setTimeout(cb, 1800); };

  return (
    <div style={{ minHeight:'100vh', height:'100vh', background:'linear-gradient(165deg,#0E1828 0%,#111F35 30%,#132240 60%,#0C1525 100%)', display:'flex', flexDirection:'column', alignItems:'center', justifyContent:'center', fontFamily:"'Inter','Segoe UI',system-ui,sans-serif", padding:'0 24px', position:'relative', overflow:'hidden' }}>
      <style>{`@keyframes lp-cardIn{from{opacity:0;transform:translateY(22px)}to{opacity:1;transform:translateY(0)}}@keyframes lp-float{0%,100%{transform:translateY(0)}50%{transform:translateY(-8px)}}@keyframes lp-ring{0%,100%{opacity:.4;transform:scale(1)}50%{opacity:.85;transform:scale(1.06)}}@keyframes lp-glow{0%,100%{opacity:.5}50%{opacity:1}}@keyframes lp-borderPulse{0%,100%{box-shadow:0 0 0 0 rgba(201,168,76,0),0 0 24px rgba(201,168,76,0.07)}50%{box-shadow:0 0 0 2px rgba(201,168,76,0.07),0 0 48px rgba(201,168,76,0.14)}}`}</style>

      <BgCanvas />

      {/* Full-page layered gold ambient lighting */}
      <div style={{ position:'absolute', inset:0, background:'radial-gradient(ellipse 140% 90% at 50% 20%,rgba(201,168,76,0.065) 0%,transparent 55%)', pointerEvents:'none', animation:'lp-glow 6s ease-in-out infinite' }} />
      <div style={{ position:'absolute', inset:0, background:'radial-gradient(ellipse 100% 70% at 15% 85%,rgba(201,168,76,0.035) 0%,transparent 50%)', pointerEvents:'none', animation:'lp-glow 8s ease-in-out infinite 2s' }} />
      <div style={{ position:'absolute', inset:0, background:'radial-gradient(ellipse 100% 70% at 85% 85%,rgba(201,168,76,0.035) 0%,transparent 50%)', pointerEvents:'none', animation:'lp-glow 7s ease-in-out infinite 1s' }} />
      <div style={{ position:'absolute', inset:0, background:'radial-gradient(ellipse 70% 50% at 50% 50%,rgba(201,168,76,0.04) 0%,transparent 55%)', pointerEvents:'none', animation:'lp-glow 5s ease-in-out infinite' }} />
      <div style={{ position:'absolute', bottom:0, left:0, right:0, height:'40%', background:'linear-gradient(to top,rgba(8,12,22,0.55),transparent)', pointerEvents:'none' }} />

      {launching && <LoadingOverlay workspace={launching} />}
      <div style={{ position:'absolute', top:0, left:0, right:0, height:'1px', background:'linear-gradient(90deg,transparent,rgba(201,168,76,0.28),transparent)', zIndex:2 }} />

      <div style={{ position:'relative', zIndex:2, display:'flex', flexDirection:'column', alignItems:'center', width:'100%', maxWidth:'1000px' }}>

        {/* LOGO */}
        <div style={{ textAlign:'center', marginBottom:'40px', animation:'lp-cardIn .6s ease', paddingTop:'16px' }}>
          <div style={{ position:'relative', display:'inline-flex', alignItems:'center', justifyContent:'center', padding:'28px' }}>
            <div style={{ position:'absolute', top:'50%', left:'50%', transform:'translate(-50%,-50%)', width:'320px', height:'320px', borderRadius:'50%', background:'radial-gradient(circle,rgba(201,168,76,0.1) 0%,rgba(201,168,76,0.02) 45%,transparent 70%)', pointerEvents:'none', animation:'lp-glow 4s ease-in-out infinite' }} />
            <div style={{ position:'relative', width:'180px', height:'180px', animation:'lp-float 5.5s ease-in-out infinite', flexShrink:0 }}>
              <div style={{ position:'absolute', inset:'-22px', borderRadius:'50%', border:'1px solid rgba(201,168,76,0.07)', animation:'lp-ring 3.5s ease-in-out infinite', pointerEvents:'none' }} />
              <div style={{ position:'absolute', inset:0, borderRadius:'50%', border:'1.5px solid rgba(201,168,76,0.28)', boxShadow:'0 0 40px rgba(201,168,76,0.1),inset 0 0 30px rgba(201,168,76,0.04)' }} />
              <div style={{ position:'absolute', inset:'10px', borderRadius:'50%', border:'1px solid rgba(201,168,76,0.09)' }} />
              <div style={{ position:'absolute', inset:0, borderRadius:'50%', background:'linear-gradient(145deg,rgba(14,24,40,0.95) 0%,rgba(11,18,32,0.98) 100%)', backdropFilter:'blur(10px)' }} />
              <div style={{ position:'absolute', inset:0, display:'flex', alignItems:'center', justifyContent:'center', padding:'22px', borderRadius:'50%', overflow:'hidden' }}>
                <img src="/logo.png" alt="National Bank of Ethiopia" style={{ width:'100%', height:'100%', objectFit:'contain', display:'block' }} />
              </div>
            </div>
          </div>
          <div style={{ marginTop:'4px' }}>
            <div style={{ fontSize:'16px', fontWeight:'700', color:'#C9A84C', letterSpacing:'.1em', textTransform:'uppercase', marginBottom:'5px' }}>National Bank of Ethiopia</div>
            <div style={{ fontSize:'10px', color:'#2D3F52', letterSpacing:'.1em', textTransform:'uppercase' }}>Blockchain Intelligence Division</div>
          </div>
        </div>

        {/* WORKSPACE PANEL — wider, more compact, animated gold border */}
        <div style={{ width:'100%', background:'rgba(10,16,28,0.68)', border:'1px solid rgba(201,168,76,0.15)', borderRadius:'20px', padding:'26px 44px 30px', backdropFilter:'blur(20px)', animation:'lp-cardIn .6s ease .15s both, lp-borderPulse 4s ease-in-out infinite' }}>
          <div style={{ textAlign:'center', marginBottom:'22px' }}>
            <div style={{ display:'inline-flex', alignItems:'center', gap:'10px', marginBottom:'10px' }}>
              <div style={{ width:'40px', height:'1px', background:'linear-gradient(90deg,transparent,rgba(201,168,76,0.32))' }} />
              <span style={{ fontSize:'10px', color:'#5A4A1E', fontWeight:'700', letterSpacing:'.12em', textTransform:'uppercase' }}>Intelligence Gateway</span>
              <div style={{ width:'40px', height:'1px', background:'linear-gradient(90deg,rgba(201,168,76,0.32),transparent)' }} />
            </div>
            <div style={{ fontSize:'20px', fontWeight:'700', color:'#C4B48A', marginBottom:'6px', letterSpacing:'-.02em' }}>Unified Blockchain Intelligence Ecosystem</div>
            <div style={{ fontSize:'12px', color:'#3A4D60', letterSpacing:'.04em' }}>Select an operational workspace to continue</div>
          </div>
          <div style={{ display:'flex', gap:'20px', flexWrap:'wrap', justifyContent:'center' }}>
            <Card amlIcon title="AML Investigation Workspace" subtitle="Financial crime monitoring, placement & layering detection, integration analysis and real-time risk intelligence." features={['Transaction Table','Wallet Clusters','Placement Alerts','Layering Alerts','Integration Alerts']} buttonLabel="Enter Workspace →" onLaunch={()=>go('aml',onEnterAML)} delay={250} />
            <Card icon="⬡" title="Wallet Analysis Workspace" subtitle="Graph intelligence, wallet clustering, ownership registry and entity investigation platform." features={['Wallet Clustering','Ownership Registry','Transaction Mapping','Graph Intelligence']} disabled buttonLabel="Awaiting Integration" delay={350} />
          </div>
        </div>

        <div style={{ marginTop:'22px', fontSize:'11px', color:'#1A2535', letterSpacing:'.05em', textAlign:'center', animation:'lp-cardIn .6s ease .4s both' }}>
          © {new Date().getFullYear()} National Bank of Ethiopia &nbsp;·&nbsp; Classified Intelligence System &nbsp;·&nbsp; All Rights Reserved
        </div>
      </div>

      <div style={{ position:'absolute', bottom:0, left:0, right:0, height:'1px', background:'linear-gradient(90deg,transparent,rgba(201,168,76,0.12),transparent)', zIndex:2 }} />
    </div>
  );
}
