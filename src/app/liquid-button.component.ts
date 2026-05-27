import { Component, ChangeDetectionStrategy, input } from '@angular/core';

@Component({
  selector: 'app-liquid-button',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <button [class]="getClasses()">
      <div class="absolute top-0 left-0 z-0 h-full w-full rounded-full 
            shadow-[0_0_6px_rgba(0,0,0,0.03),0_2px_6px_rgba(0,0,0,0.08),inset_3px_3px_0.5px_-3px_rgba(0,0,0,0.9),inset_-3px_-3px_0.5px_-3px_rgba(0,0,0,0.85),inset_1px_1px_1px_-0.5px_rgba(0,0,0,0.6),inset_-1px_-1px_1px_-0.5px_rgba(0,0,0,0.6),inset_0_0_6px_6px_rgba(0,0,0,0.12),inset_0_0_2px_2px_rgba(0,0,0,0.06),0_0_12px_rgba(255,255,255,0.15)] 
        transition-all 
        dark:shadow-[0_0_8px_rgba(0,0,0,0.03),0_2px_6px_rgba(0,0,0,0.08),inset_3px_3px_0.5px_-3.5px_rgba(255,255,255,0.09),inset_-3px_-3px_0.5px_-3.5px_rgba(255,255,255,0.85),inset_1px_1px_1px_-0.5px_rgba(255,255,255,0.6),inset_-1px_-1px_1px_-0.5px_rgba(255,255,255,0.6),inset_0_0_6px_6px_rgba(255,255,255,0.12),inset_0_0_2px_2px_rgba(255,255,255,0.06),0_0_12px_rgba(0,0,0,0.15)]"></div>
      
      <div class="absolute top-0 left-0 isolate -z-10 h-full w-full overflow-hidden rounded-full"
           style="backdrop-filter: url('#container-glass')"></div>

      <div class="pointer-events-none z-10 relative">
        <ng-content></ng-content>
      </div>

      <svg class="hidden" width="0" height="0">
        <defs>
          <filter id="container-glass" x="0%" y="0%" width="100%" height="100%" color-interpolation-filters="sRGB">
            <feTurbulence type="fractalNoise" baseFrequency="0.05 0.05" numOctaves="1" seed="1" result="turbulence" />
            <feGaussianBlur in="turbulence" stdDeviation="2" result="blurredNoise" />
            <feDisplacementMap in="SourceGraphic" in2="blurredNoise" scale="70" xChannelSelector="R" yChannelSelector="B" result="displaced" />
            <feGaussianBlur in="displaced" stdDeviation="4" result="finalBlur" />
            <feComposite in="finalBlur" in2="finalBlur" operator="over" />
          </filter>
        </defs>
      </svg>
    </button>
  `,
  host: {
    'class': 'inline-flex'
  }
})
export class LiquidButtonComponent {
  variant = input<'default' | 'destructive' | 'outline'>('default');
  size = input<'sm' | 'default' | 'lg' | 'xl' | 'xxl'>('xxl');
  baseClasses = 'relative inline-flex items-center transition-colors justify-center cursor-pointer gap-2 whitespace-nowrap text-sm font-medium transition-[color,box-shadow] disabled:pointer-events-none disabled:opacity-50 outline-none focus-visible:border-ring focus-visible:ring-ring/50 focus-visible:ring-[3px] aria-invalid:ring-destructive/20 dark:aria-invalid:ring-destructive/40 aria-invalid:border-destructive text-white border border-white/20 rounded-full';

  getClasses() {
    let classes = this.baseClasses;

    switch(this.variant()) {
      case 'default':
        classes += ' bg-transparent hover:scale-105 duration-300 transition text-white';
        break;
      case 'outline':
        classes += ' border border-input bg-background hover:bg-accent hover:text-accent-foreground';
        break;
      case 'destructive':
        classes += ' bg-destructive text-white hover:bg-destructive/90 focus-visible:ring-destructive/20 dark:focus-visible:ring-destructive/40';
        break;
    }

    switch(this.size()) {
      case 'sm':
        classes += ' h-8 text-xs gap-1.5 px-4';
        break;
      case 'default':
        classes += ' h-9 px-4 py-2';
        break;
      case 'lg':
        classes += ' h-10 px-6';
        break;
      case 'xl':
        classes += ' h-12 px-8';
        break;
      case 'xxl':
        classes += ' h-14 px-10';
        break;
    }

    return classes;
  }
}
