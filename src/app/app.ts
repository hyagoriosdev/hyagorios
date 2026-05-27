import {ChangeDetectionStrategy, Component} from '@angular/core';
import {WebGLShaderComponent} from './web-gl-shader.component';
import {LiquidButtonComponent} from './liquid-button.component';

@Component({
  changeDetection: ChangeDetectionStrategy.OnPush,
  selector: 'app-root',
  imports: [WebGLShaderComponent, LiquidButtonComponent],
  templateUrl: './app.html',
  styleUrl: './app.css',
})
export class App {}
