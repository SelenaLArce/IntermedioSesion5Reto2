package org.example;

import reactor.core.publisher.Flux;
import java.time.Duration;
import java.util.Random;

public class UCI {

    public static void main(String[] args) throws InterruptedException {
        Random random = new Random();

        Flux<EventoVital> paciente1 = generarSignosVitales(1, random);
        Flux<EventoVital> paciente2 = generarSignosVitales(2, random);
        Flux<EventoVital> paciente3 = generarSignosVitales(3, random);

        Flux.merge(paciente1, paciente2, paciente3)
                .filter(EventoVital::esCritico)
                .sort((e1, e2) -> e1.prioridad() - e2.prioridad())
                .delayElements(Duration.ofSeconds(1))
                .subscribe(System.out::println);

        Thread.sleep(15000);
    }

    private static Flux<EventoVital> generarSignosVitales(int pacienteId, Random random) {
        return Flux.interval(Duration.ofMillis(300))
                .map(tick -> {
                    int fc = 40 + random.nextInt(100);
                    int pas = 80 + random.nextInt(80);
                    int pad = 50 + random.nextInt(50);
                    int spo2 = 85 + random.nextInt(15);

                    return new EventoVital(pacienteId, fc, pas, pad, spo2);
                })
                .take(10);
    }

    static class EventoVital {
        private final int pacienteId;
        private final int fc;
        private final int pas;
        private final int pad;
        private final int spo2;

        public EventoVital(int pacienteId, int fc, int pas, int pad, int spo2) {
            this.pacienteId = pacienteId;
            this.fc = fc;
            this.pas = pas;
            this.pad = pad;
            this.spo2 = spo2;
        }

        public boolean esCritico() {
            return fc < 50 || fc > 120 || pas < 90 || pas > 140 || pad < 60 || pad > 90 || spo2 < 90;
        }

        public int prioridad() {
            if (fc < 50 || fc > 120) return 1;
            if (spo2 < 90) return 2;
            return 3;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            if (fc < 50 || fc > 120) sb.append("Paciente ").append(pacienteId).append(" - FC crítica: ").append(fc).append(" bpm\n");
            if (spo2 < 90) sb.append("Paciente ").append(pacienteId).append(" - SpO2 baja: ").append(spo2).append("%\n");
            if (pas < 90 || pas > 140 || pad < 60 || pad > 90)
                sb.append("Paciente ").append(pacienteId).append(" - PA crítica: ").append(pas).append("/").append(pad).append(" mmHg\n");
            return sb.toString().trim();
        }
    }
}