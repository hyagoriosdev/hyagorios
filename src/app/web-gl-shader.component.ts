import { Component, ElementRef, ViewChild, AfterViewInit, OnDestroy, ChangeDetectionStrategy, NgZone, inject, PLATFORM_ID } from '@angular/core';
import { isPlatformBrowser } from '@angular/common';
import * as THREE from 'three';

@Component({
  selector: 'app-web-gl-shader',
  standalone: true,
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `<canvas #canvasRef class="fixed top-0 left-0 w-full h-full block z-0 pointer-events-none"></canvas>`
})
export class WebGLShaderComponent implements AfterViewInit, OnDestroy {
  @ViewChild('canvasRef') canvasRef!: ElementRef<HTMLCanvasElement>;

  private scene: THREE.Scene | null = null;
  private camera: THREE.OrthographicCamera | null = null;
  private renderer: THREE.WebGLRenderer | null = null;
  private mesh: THREE.Mesh | null = null;
  private uniforms: {
    resolution: { value: [number, number] },
    time: { value: number }
  } | null = null;
  private animationId: number | null = null;
  private ngZone = inject(NgZone);
  private platformId = inject(PLATFORM_ID);

  ngAfterViewInit() {
    if (isPlatformBrowser(this.platformId)) {
      this.ngZone.runOutsideAngular(() => {
        this.initScene();
        this.animate();
        window.addEventListener("resize", this.handleResize);
      });
    }
  }

  ngOnDestroy() {
    if (isPlatformBrowser(this.platformId)) {
      if (this.animationId) cancelAnimationFrame(this.animationId);
      window.removeEventListener("resize", this.handleResize);
      if (this.mesh) {
        this.scene?.remove(this.mesh);
        this.mesh.geometry.dispose();
        if (this.mesh.material instanceof THREE.Material) {
          this.mesh.material.dispose();
        }
      }
      this.renderer?.dispose();
    }
  }

  private initScene() {
    const canvas = this.canvasRef.nativeElement;

    const vertexShader = `
      attribute vec3 position;
      void main() {
        gl_Position = vec4(position, 1.0);
      }
    `;

    const fragmentShader = `
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
    `;

    this.scene = new THREE.Scene();
    this.renderer = new THREE.WebGLRenderer({ canvas });
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.setClearColor(new THREE.Color(0x000000));

    this.camera = new THREE.OrthographicCamera(-1, 1, 1, -1, 0, -1);

    const pixelRatio = window.devicePixelRatio || 1;
    this.uniforms = {
      resolution: { value: [window.innerWidth * pixelRatio, window.innerHeight * pixelRatio] },
      time: { value: 0.0 },
    };

    const position = [
      -1.0, -1.0, 0.0,
       1.0, -1.0, 0.0,
      -1.0,  1.0, 0.0,
       1.0, -1.0, 0.0,
      -1.0,  1.0, 0.0,
       1.0,  1.0, 0.0,
    ];

    const positions = new THREE.BufferAttribute(new Float32Array(position), 3);
    const geometry = new THREE.BufferGeometry();
    geometry.setAttribute("position", positions);

    const material = new THREE.RawShaderMaterial({
      vertexShader,
      fragmentShader,
      uniforms: this.uniforms,
      side: THREE.DoubleSide,
    });

    this.mesh = new THREE.Mesh(geometry, material);
    this.scene.add(this.mesh);

    this.handleResize();
  }

  private animate = () => {
    if (this.uniforms) this.uniforms.time.value += 0.008;
    if (this.renderer && this.scene && this.camera) {
      this.renderer.render(this.scene, this.camera);
    }
    this.animationId = requestAnimationFrame(this.animate);
  }

  private handleResize = () => {
    if (!this.renderer || !this.uniforms) return;
    const width = window.innerWidth;
    const height = window.innerHeight;
    const pixelRatio = window.devicePixelRatio || 1;
    this.renderer.setSize(width, height, false);
    this.uniforms.resolution.value = [width * pixelRatio, height * pixelRatio];
  }
}
